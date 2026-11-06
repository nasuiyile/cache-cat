use crate::protocol::key::scan::ScanParams;
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::store::statemachine::{RaftMetaData, SnapshotState};
use crate::raft::types::core::mocha::core::{MyCache, MyValue};
use crate::raft::types::core::mocha::request_handler::read_request;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;

fn scan_all(cache: &MyCache, db_number: u16) -> BTreeSet<Bytes> {
    let mut params = ScanParams {
        cursor: 0,
        count: 3,
        pattern: None,
        type_filter: Some("string".into()),
    };
    let mut keys = BTreeSet::new();
    for _ in 0..16384 {
        let Value::Array(Some(reply)) =
            read_request(cache, ReadOperation::Scan(params.clone()), db_number, 200)
        else {
            panic!("expected SCAN array");
        };
        params.cursor = std::str::from_utf8(&reply[0].string_bytes_clone().unwrap())
            .unwrap()
            .parse()
            .unwrap();
        let Value::Array(Some(page)) = &reply[1] else {
            panic!("expected SCAN key array")
        };
        for key in page {
            keys.insert(key.string_bytes_clone().unwrap());
        }
        if params.cursor == 0 {
            return keys;
        }
    }
    panic!("SCAN did not finish");
}

#[tokio::test]
async fn scan_restarts_after_disk_snapshot_and_covers_restored_mutations() {
    let source = Arc::new(MyCache::new(2).unwrap());
    source.pause_expire_workers();
    source.set_write_clock(100);
    let mut expected: BTreeSet<Bytes> = (0..1024).map(|i| format!("key:{i}").into()).collect();
    expected.extend([Bytes::new(), Bytes::from_static(b"binary\0\xff")]);
    let storage = &source.databases[1].mocha;
    for key in &expected {
        storage.insert_persistent(key.clone(), MyValue::new(ValueObject::Int(1)));
    }
    for i in 0..900 {
        let key: Bytes = format!("key:{i}").into();
        storage.remove(&key);
        expected.remove(&key);
    }
    storage.insert_absolute("expired".into(), MyValue::new(ValueObject::Int(1)), 200);
    source.databases[0]
        .mocha
        .insert_persistent("other-db".into(), MyValue::new(ValueObject::Int(2)));
    assert_eq!(scan_all(&source, 1), expected);
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
    let restored = Arc::new(MyCache::new(2).unwrap());
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
    assert_eq!(scan_all(&restored, 1), expected);
    assert_eq!(scan_all(&restored, 0), BTreeSet::from(["other-db".into()]));
    for i in 0..512 {
        let key: Bytes = format!("new:{i}").into();
        restored.databases[1].mocha.insert_persistent(
            key.clone(),
            MyValue::new(ValueObject::String("restored".into())),
        );
        expected.insert(key);
    }
    restored.databases[1].mocha.remove(&Bytes::new());
    expected.remove(b"".as_slice());
    assert_eq!(scan_all(&restored, 1), expected);
    assert_eq!(scan_all(&source, 1).len(), 126);
}
