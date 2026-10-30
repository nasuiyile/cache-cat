use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hscan::HScanCommand;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::store::statemachine::{RaftMetaData, SnapshotState};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

fn apply(cache: &MyCache, operation: BaseOperation) {
    let mut update_type = UpdateType::None;
    let mut update = Update {
        db_number: 0,
        write_clock: cache.set_write_clock(100),
        update_type: &mut update_type,
        effects: None,
        waiters: None,
    };
    let reply = base_request(cache, operation, &mut update);
    assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
}

fn scan_all(cache: &MyCache) -> BTreeMap<Bytes, Bytes> {
    let mut cursor = Bytes::from_static(b"0");
    let mut fields = BTreeMap::new();
    for _ in 0..4096 {
        let args = [
            Bytes::from_static(b"HSCAN"),
            Bytes::from_static(b"hash"),
            cursor,
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"3"),
        ]
        .into_iter()
        .map(|arg| Value::BulkString(Some(arg)))
        .collect::<Vec<_>>();
        let operation = HScanCommand.read_operation(&args).unwrap();
        let Value::Array(Some(reply)) = read_request(cache, operation, 0, 100) else {
            panic!("expected HSCAN array");
        };
        cursor = reply[0].string_bytes_clone().unwrap();
        let Value::Array(Some(entries)) = &reply[1] else {
            panic!("expected HSCAN field/value array");
        };
        assert_eq!(entries.len() % 2, 0);
        for pair in entries.chunks_exact(2) {
            assert!(
                fields
                    .insert(
                        pair[0].string_bytes_clone().unwrap(),
                        pair[1].string_bytes_clone().unwrap(),
                    )
                    .is_none(),
                "an unchanged hash must visit each field once"
            );
        }
        if cursor.as_ref() == b"0" {
            return fields;
        }
    }
    panic!("HSCAN did not finish");
}

#[tokio::test]
async fn hscan_hash_survives_disk_snapshot_after_growth_and_deletion() {
    let source = Arc::new(MyCache::new(1).unwrap());
    source.pause_expire_workers();
    let mut expected: BTreeMap<Bytes, Bytes> = (0..1024)
        .map(|i| (format!("field:{i}").into(), i.to_string().into()))
        .collect();
    expected.insert(Bytes::new(), Bytes::from_static(b"01"));
    expected.insert(
        Bytes::from_static(b"binary\0\xff"),
        Bytes::from_static(b"value\0\xff"),
    );
    apply(
        &source,
        BaseOperation::HSet(HSetReq {
            key: "hash".into(),
            elements: expected
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect(),
        }),
    );
    let removed: Vec<Bytes> = (0..900).map(|i| format!("field:{i}").into()).collect();
    apply(
        &source,
        BaseOperation::HDel(HDelReq {
            key: "hash".into(),
            fields: removed.clone(),
        }),
    );
    for field in removed {
        expected.remove(&field);
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
    for (field, value) in &expected {
        let reply = restored.execute_read(
            HGetParams {
                key: "hash".into(),
                field: field.clone(),
            },
            0,
            100,
        );
        assert_eq!(reply.string_bytes_clone().as_ref(), Some(value));
    }

    let added: Vec<(Bytes, Bytes)> = (0..512)
        .map(|i| (format!("new:{i}").into(), Bytes::from_static(b"restored")))
        .collect();
    apply(
        &restored,
        BaseOperation::HSet(HSetReq {
            key: "hash".into(),
            elements: added.clone(),
        }),
    );
    expected.extend(added);
    apply(
        &restored,
        BaseOperation::HDel(HDelReq {
            key: "hash".into(),
            fields: vec![Bytes::from_static(b"field:900"), Bytes::new()],
        }),
    );
    expected.remove(b"field:900".as_slice());
    expected.remove(b"".as_slice());
    assert_eq!(scan_all(&restored), expected);
    assert_eq!(
        scan_all(&source).len(),
        126,
        "restored mutations must not affect the source"
    );
}
