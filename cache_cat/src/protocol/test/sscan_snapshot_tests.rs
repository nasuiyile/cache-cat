use crate::protocol::raft_command::ReadRaftCommand;
use crate::protocol::set::sadd::SAddReq;
use crate::protocol::set::srem::SRemReq;
use crate::protocol::set::sscan::SScanCommand;
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::store::statemachine::{RaftMetaData, SnapshotState};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::AtomicRequest;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;

fn apply<'a>(cache: &MyCache, operation: BaseOperation, update_type: &'a mut UpdateType<'a>) {
    let mut update = Update {
        db_number: 0,
        write_clock: cache.set_write_clock(100),
        update_type,
        effects: None,
        waiters: None,
    };
    let reply = base_request(cache, operation, &mut update);
    assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
}

fn add(members: Vec<Bytes>) -> BaseOperation {
    BaseOperation::SAdd(SAddReq {
        key: "set".into(),
        elements: members,
    })
}

fn remove(members: Vec<Bytes>) -> BaseOperation {
    BaseOperation::SRem(SRemReq {
        key: "set".into(),
        members,
    })
}

fn scan_page(cache: &MyCache, cursor: u64) -> (u64, Vec<Bytes>) {
    let args = [
        Bytes::from_static(b"SSCAN"),
        Bytes::from_static(b"set"),
        cursor.to_string().into(),
        Bytes::from_static(b"COUNT"),
        Bytes::from_static(b"3"),
    ]
    .into_iter()
    .map(|arg| Value::BulkString(Some(arg)))
    .collect::<Vec<_>>();
    let operation = SScanCommand.read_operation(&args).unwrap();
    let Value::Array(Some(reply)) = read_request(cache, operation, 0, 100) else {
        panic!("expected SSCAN array");
    };
    let cursor = reply[0].string_bytes_clone().unwrap();
    let cursor = std::str::from_utf8(&cursor).unwrap().parse().unwrap();
    let Value::Array(Some(members)) = &reply[1] else {
        panic!("expected SSCAN member array");
    };
    (
        cursor,
        members
            .iter()
            .map(|member| member.string_bytes_clone().unwrap())
            .collect(),
    )
}

fn scan_all(cache: &MyCache) -> BTreeSet<Bytes> {
    let mut cursor = 0;
    let mut members = BTreeSet::new();
    for _ in 0..4096 {
        let (next, page) = scan_page(cache, cursor);
        for member in page {
            assert!(
                members.insert(member),
                "unchanged set visited a member twice"
            );
        }
        cursor = next;
        if cursor == 0 {
            return members;
        }
    }
    panic!("SSCAN did not finish");
}

#[test]
fn sscan_cursor_survives_snapshot_copy_on_write_and_clones_stay_isolated() {
    let source = MyCache::new(1).unwrap();
    source.pause_expire_workers();
    let expected: BTreeSet<Bytes> = (0..128).map(|i| format!("member:{i}").into()).collect();
    apply(
        &source,
        add(expected.iter().cloned().collect()),
        &mut UpdateType::None,
    );
    let frozen = source.databases[0]
        .mocha
        .get_entry(b"set".as_slice())
        .unwrap();
    let ValueObject::Set(original) = &frozen.value.data else {
        panic!("expected set")
    };
    let reference = MyCache::new(1).unwrap();
    reference.pause_expire_workers();
    reference.set_write_clock(100);
    reference.databases[0]
        .mocha
        .insert_snapshot("set".into(), frozen.clone());

    let mut queue = Vec::new();
    let mut revision = 0;
    let mut cursor = 0;
    let mut seen = BTreeSet::new();
    for _ in 0..4096 {
        // Even an existing member takes the snapshot cloning path. A newly
        // seeded clone would change pages/cursors despite identical members.
        apply(
            &source,
            add(vec!["member:0".into()]),
            &mut UpdateType::Snapshot {
                queue: &mut queue,
                revision: &mut revision,
            },
        );
        let page = scan_page(&source, cursor);
        assert_eq!(page, scan_page(&reference, cursor));
        seen.extend(page.1);
        cursor = page.0;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(cursor, 0);
    assert_eq!(seen, expected);

    apply(
        &source,
        add(vec!["later".into()]),
        &mut UpdateType::Snapshot {
            queue: &mut queue,
            revision: &mut revision,
        },
    );
    apply(
        &source,
        remove(vec!["member:0".into()]),
        &mut UpdateType::Snapshot {
            queue: &mut queue,
            revision: &mut revision,
        },
    );
    assert_eq!(
        original.lock().iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    let mut final_members = expected;
    final_members.remove(b"member:0".as_slice());
    final_members.insert("later".into());
    assert_eq!(scan_all(&source), final_members);

    let bytes = bincode2::serialize(&queue).unwrap();
    let queue: Vec<AtomicRequest> = bincode2::deserialize(&bytes).unwrap();
    for delta in queue {
        apply(
            &reference,
            delta.request,
            &mut UpdateType::CAS {
                expected_revision: delta.expected_revision,
                revision: delta.version,
            },
        );
    }
    assert_eq!(scan_all(&reference), final_members);
}

#[tokio::test]
async fn sscan_set_survives_disk_snapshot_after_growth_and_deletion() {
    let source = Arc::new(MyCache::new(1).unwrap());
    source.pause_expire_workers();
    let mut expected: BTreeSet<Bytes> = (0..1024).map(|i| format!("member:{i}").into()).collect();
    expected.insert(Bytes::new());
    expected.insert(Bytes::from_static(b"binary\0\xff"));
    apply(
        &source,
        add(expected.iter().cloned().collect()),
        &mut UpdateType::None,
    );
    let removed: Vec<Bytes> = (0..900).map(|i| format!("member:{i}").into()).collect();
    apply(&source, remove(removed.clone()), &mut UpdateType::None);
    for member in removed {
        expected.remove(&member);
    }
    assert_eq!(scan_all(&source), expected);

    let directory = tempfile::tempdir().unwrap();
    let metadata = Arc::new(Mutex::new(RaftMetaData {
        snapshot_state: SnapshotState::Start,
        snapshot_revision: 0,
        last_applied_log_id: None,
        last_membership: Default::default(),
    }));
    dump_cache_to_path(
        source.clone(),
        directory.path(),
        metadata,
        Arc::new(Mutex::new(Vec::new())),
    )
    .await
    .unwrap();
    let restored = Arc::new(MyCache::new(1).unwrap());
    restored.pause_expire_workers();
    let (_, queue, clock, _) = load_cache_from_path(
        restored.clone(),
        directory
            .path()
            .join("snapshot")
            .join(get_snapshot_file_name()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(queue.is_empty());
    restored.set_write_clock(clock);
    assert_eq!(scan_all(&restored), expected);

    let added: Vec<Bytes> = (0..512).map(|i| format!("new:{i}").into()).collect();
    apply(&restored, add(added.clone()), &mut UpdateType::None);
    expected.extend(added);
    apply(
        &restored,
        remove(vec!["member:900".into(), Bytes::new()]),
        &mut UpdateType::None,
    );
    expected.remove(b"member:900".as_slice());
    expected.remove(b"".as_slice());
    assert_eq!(scan_all(&restored), expected);
    assert_eq!(scan_all(&source).len(), 126);
}

#[test]
fn sscan_continues_when_snapshot_writes_compact_a_sparse_set() {
    let cache = MyCache::new(1).unwrap();
    cache.pause_expire_workers();
    apply(
        &cache,
        add((0..2048).map(|i| format!("member:{i}").into()).collect()),
        &mut UpdateType::None,
    );
    apply(
        &cache,
        remove((0..1920).map(|i| format!("member:{i}").into()).collect()),
        &mut UpdateType::None,
    );
    let stable: BTreeSet<Bytes> = (1920..2048).map(|i| format!("member:{i}").into()).collect();
    let frozen = cache.databases[0]
        .mocha
        .get_entry(b"set".as_slice())
        .unwrap();
    let ValueObject::Set(original) = &frozen.value.data else {
        panic!("expected set");
    };
    let original_bytes = original.lock().allocated_bytes();
    let (mut cursor, first_page) = scan_page(&cache, 0);
    assert_ne!(cursor, 0);
    let mut seen: BTreeSet<_> = first_page.into_iter().collect();
    let mut queue = Vec::new();
    let mut revision = 0;
    for step in 0..4096 {
        let operation = if step % 2 == 0 {
            add(vec!["temporary".into()])
        } else {
            remove(vec!["temporary".into()])
        };
        apply(
            &cache,
            operation,
            &mut UpdateType::Snapshot {
                queue: &mut queue,
                revision: &mut revision,
            },
        );
        if step == 0 {
            let entry = cache.databases[0]
                .mocha
                .get_entry(b"set".as_slice())
                .unwrap();
            let ValueObject::Set(compacted) = entry.value.data else {
                panic!("expected set");
            };
            assert!(compacted.lock().allocated_bytes() < original_bytes);
        }
        let (next, members) = scan_page(&cache, cursor);
        // Shrinking may repeat members, as permitted by Redis SSCAN.
        seen.extend(members);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(cursor, 0);
    assert!(stable.is_subset(&seen));
    assert_eq!(
        original.lock().iter().cloned().collect::<BTreeSet<_>>(),
        stable,
        "snapshot source must stay unchanged"
    );
}
