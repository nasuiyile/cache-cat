//! SPOP relies on replicated dense member order, not local hash-table layout.

use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::store::statemachine::{RaftMetaData, SnapshotState};
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_set::ScanSet;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

fn request(args: &[&str]) -> Operation {
    let values = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua()
        .parse_request(&values)
        .unwrap()
}

fn replica(seed: u64, capacity: usize) -> MyCache {
    let cache = MyCache::new(1).unwrap();
    cache.pause_expire_workers();
    cache.set_write_clock(100);
    for (offset, key) in ["bag", "late", "other"].into_iter().enumerate() {
        let seed = seed + offset as u64 * 4;
        let mut set = ScanSet::with_capacity_and_hasher(
            capacity,
            RandomState::with_seeds(seed, seed + 1, seed + 2, seed + 3),
        );
        // Keep the same insertion history on all replicas, deliberately
        // different from byte order and independent of seed/capacity.
        for index in 0..96 {
            if key != "other" || index % 2 == 0 {
                set.insert(format!("member:{}", index * 37 % 96).into());
            }
        }
        cache.databases[0].mocha.insert_absolute(
            Bytes::copy_from_slice(key.as_bytes()),
            MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
            10_000,
        );
    }
    cache
}

fn set(cache: &MyCache, key: &str) -> Arc<Mutex<ScanSet<Bytes>>> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
    let ValueObject::Set(set) = entry.value.data else {
        panic!("expected Set at {key}");
    };
    set
}

fn order(cache: &MyCache, key: &str) -> Option<Vec<Bytes>> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes())?;
    let ValueObject::Set(set) = entry.value.data else {
        panic!("expected Set at {key}");
    };
    Some(set.lock().iter().cloned().collect())
}

fn assert_same_sets(left: &MyCache, right: &MyCache, keys: &[&str]) {
    for key in keys {
        assert_eq!(order(left, key), order(right, key), "member order at {key}");
    }
}

fn apply<'a>(
    cache: &MyCache,
    operation: Operation,
    clock: u64,
    update_type: &'a mut UpdateType<'a>,
) -> Value {
    let mut update = Update {
        db_number: 0,
        write_clock: cache.set_write_clock(clock),
        update_type,
        effects: None,
        waiters: None,
    };
    let reply = do_request(cache, operation, &mut update, true);
    assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
    reply
}

#[test]
fn spop_history_preserves_order_across_local_layouts_and_snapshot_clones() {
    let snapshotter = replica(1, 4096);
    let follower = replica(101, 128);
    let frozen = set(&snapshotter, "bag");
    let original_order = frozen.lock().iter().cloned().collect::<Vec<_>>();
    let original_bytes = frozen.lock().allocated_bytes();
    assert_ne!(
        frozen.lock().hasher().hash_one(b"probe"),
        set(&follower, "bag").lock().hasher().hash_one(b"probe")
    );

    let mut operations = [
        vec!["SADD", "bag", "member:0", "member:0", "new", "new"],
        vec!["SREM", "bag", "member:7", "member:7", "missing"],
        vec!["SUNIONSTORE", "union", "bag", "other"],
        vec!["SINTERSTORE", "intersection", "union", "other"],
        vec!["SDIFFSTORE", "difference", "union", "other"],
        vec!["RENAME", "difference", "renamed"],
        vec!["SPOP", "bag"],
        vec!["SPOP", "bag", "7"],
        vec!["SPOP", "union", "3"],
        vec!["SPOP", "intersection", "1"],
        vec!["SPOP", "renamed", "2"],
    ]
    .iter()
    .map(|args| request(args))
    .collect::<Vec<_>>();
    operations.push(Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
        "local members = redis.call('SPOP', KEYS[1], 3); redis.call('SADD', KEYS[2], unpack(members)); return {members, redis.call('SPOP', KEYS[1])}".into(),
        2,
        vec!["bag".into(), "saved".into()],
        Vec::new(),
    ))));
    operations.push(Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: [
            vec!["SADD", "bag", "transaction", "transaction"],
            vec!["SPOP", "bag", "2"],
            vec!["SPOP", "bag"],
            vec!["SUNIONSTORE", "union", "bag", "saved"],
            vec!["SPOP", "union", "5"],
        ]
        .iter()
        .map(|args| QueuedOperation::new(0, request(args)))
        .collect(),
    })));
    operations.extend([
        request(&["SPOP", "renamed", "10000"]),
        request(&["SPOP", "renamed"]),
        request(&["SPOP", "bag", "0"]),
    ]);

    let mut queue = Vec::new();
    let mut revision = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let clock = 200 + index as u64;
        let left = apply(
            &snapshotter,
            operation.clone(),
            clock,
            &mut UpdateType::Snapshot {
                queue: &mut queue,
                revision: &mut revision,
            },
        );
        let right = apply(&follower, operation, clock, &mut UpdateType::None);
        for proto in [2, 3] {
            assert_eq!(left.encode_proto(proto), right.encode_proto(proto));
        }
        if index == 11 {
            assert_eq!(order(&snapshotter, "saved").unwrap().len(), 3);
        }
        if index == 12 {
            let Value::Array(Some(replies)) = &left else {
                panic!("expected EXEC replies: {left:?}");
            };
            assert_eq!(replies.len(), 5);
            assert!(
                replies
                    .iter()
                    .all(|reply| !matches!(reply, Value::Error(_)))
            );
        }
        assert_same_sets(
            &snapshotter,
            &follower,
            &[
                "bag",
                "late",
                "other",
                "union",
                "intersection",
                "renamed",
                "saved",
            ],
        );
        if index == 0 {
            assert!(set(&snapshotter, "bag").lock().allocated_bytes() < original_bytes / 2);
        }
    }
    assert_eq!(
        frozen.lock().iter().cloned().collect::<Vec<_>>(),
        original_order
    );
}

#[tokio::test]
async fn spop_order_survives_disk_snapshot_with_applied_and_skipped_cas_deltas() {
    let source = Arc::new(replica(3, 4096));
    let full_view = Arc::new(MyCache::new(1).unwrap());
    full_view.pause_expire_workers();
    for key in ["bag", "late", "other"] {
        let entry = source.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
        full_view.databases[0]
            .mocha
            .insert_snapshot(key.into(), entry);
    }

    let mut queue = Vec::new();
    let mut revision = 0;
    for (index, args) in [
        vec!["SPOP", "bag", "5"],
        vec!["SADD", "bag", "during", "during"],
        vec!["SPOP", "late", "3"],
        vec!["SREM", "bag", "member:23"],
        vec!["SUNIONSTORE", "stored", "bag", "other"],
        vec!["SPOP", "stored", "4"],
        vec!["SPOP", "bag", "2"],
        vec!["SADD", "late", "after-full-scan"],
    ]
    .iter()
    .enumerate()
    {
        apply(
            &source,
            request(args),
            200 + index as u64,
            &mut UpdateType::Snapshot {
                queue: &mut queue,
                revision: &mut revision,
            },
        );
        if index == 2 {
            // Model a full traversal that saw bag before its first write,
            // late after its first SPOP, and stored before it was created.
            // Snapshot writes isolate these captured mutable payloads.
            let entry = source.databases[0]
                .mocha
                .get_entry(b"late".as_slice())
                .unwrap();
            full_view.databases[0]
                .mocha
                .insert_snapshot("late".into(), entry);
        }
    }
    assert_eq!(queue.len(), 8);
    let clock = source.get_write_clock();
    full_view.set_write_clock(clock);
    let directory = tempfile::tempdir().unwrap();
    dump_cache_to_path(
        full_view,
        directory.path(),
        Arc::new(AsyncMutex::new(RaftMetaData {
            snapshot_state: SnapshotState::Start,
            snapshot_revision: revision,
            last_applied_log_id: None,
            last_membership: Default::default(),
        })),
        Arc::new(AsyncMutex::new(queue)),
    )
    .await
    .unwrap();

    let restored = Arc::new(MyCache::new(1).unwrap());
    restored.pause_expire_workers();
    let (_, queue, saved_clock, saved_revision) = load_cache_from_path(
        restored.clone(),
        directory
            .path()
            .join("snapshot")
            .join(get_snapshot_file_name()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(saved_revision, revision);
    let mut skipped = 0;
    for delta in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: delta.expected_revision,
            revision: delta.version,
        };
        let mut update = Update {
            db_number: delta.db_number,
            write_clock: restored.set_write_clock(delta.write_clock),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let already_in_full = matches!(
            &delta.request,
            BaseOperation::SPop(params) if params.key.as_ref() == b"late"
        );
        let reply = base_request(&restored, delta.request, &mut update);
        if already_in_full {
            assert!(matches!(reply, Value::Null));
            skipped += 1;
        } else {
            assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        }
    }
    assert_eq!(skipped, 1);
    restored.set_write_clock(saved_clock);
    assert_same_sets(&source, &restored, &["bag", "late", "other", "stored"]);

    for (index, args) in [
        vec!["SPOP", "bag"],
        vec!["SPOP", "late", "9"],
        vec!["SPOP", "stored", "7"],
        vec!["SADD", "bag", "after-restore"],
        vec!["SPOP", "bag", "10000"],
        vec!["SPOP", "stored"],
    ]
    .iter()
    .enumerate()
    {
        let operation = request(args);
        let clock = saved_clock + index as u64 + 1;
        let left = apply(&source, operation.clone(), clock, &mut UpdateType::None);
        let right = apply(&restored, operation, clock, &mut UpdateType::None);
        assert_eq!(left.encode(), right.encode());
        assert_same_sets(&source, &restored, &["bag", "late", "other", "stored"]);
    }
}
