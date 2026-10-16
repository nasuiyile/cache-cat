use super::snapshot_handler::{
    CacheCatSnapshotMeta, dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::cfg::config::Config;
use crate::node::parsed_config::ParsedConfig;
use crate::protocol::key::persist::PersistReq;
use crate::raft::store::statemachine::{SnapshotState, StateMachineStore};
use crate::raft::types::core::mocha::core::{MyCache, MyValue};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, Request};
use crate::raft::types::file_operator::load_meta_from_path;
use crate::raft::types::raft_types::{
    Entry, LeaderId, LogId, Node, NodeId, SnapshotMeta, StoredMembership,
};
use bytes::Bytes;
use openraft::Membership;
use openraft::entry::RaftEntry;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

const LARGE_VALUE: &[u8] = &[b'x'; 16 * 1024];

fn log_id(index: u64) -> LogId {
    LogId::new(
        LeaderId {
            term: 3,
            node_id: 1,
        },
        index,
    )
}

fn membership(
    configs: Vec<BTreeSet<NodeId>>,
    node_count: NodeId,
    long_dns: bool,
) -> Membership<NodeId, Node> {
    let nodes = (1..=node_count)
        .map(|node_id| {
            let address = if long_dns {
                format!(
                    "cache-node-{node_id}.{}.{}.cluster.example.com",
                    "a".repeat(50),
                    "b".repeat(50)
                )
            } else {
                format!("127.0.0.{node_id}")
            };
            (
                node_id,
                Node {
                    node_id,
                    endpoint: Endpoint::new(address, 6379, 6380),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    Membership::new(configs, nodes).unwrap()
}

async fn store(path: &Path) -> StateMachineStore {
    let mut config = Config::default();
    config.redis.databases = 1;
    StateMachineStore::new(ParsedConfig::from(&config).unwrap(), path.to_path_buf(), 1)
        .await
        .unwrap()
}

fn assert_value(cache: &MyCache) {
    let entry = cache.databases[0]
        .mocha
        .get_entry(b"ttl-key".as_slice())
        .unwrap();
    assert_eq!(entry.expire_at, Some(5_000));
    assert_eq!(entry.value.version, 7);
    match entry.value.data {
        ValueObject::String(value) => assert_eq!(value.as_ref(), b"snapshot value"),
        value => panic!("unexpected restored value: {value:?}"),
    }
    let large = cache.databases[0]
        .mocha
        .get(b"large-key".as_slice())
        .unwrap();
    match large.data {
        ValueObject::String(value) => assert_eq!(value.as_ref(), LARGE_VALUE),
        value => panic!("unexpected restored large value: {value:?}"),
    }
}

#[tokio::test]
async fn large_membership_survives_build_restart_and_install() {
    let memberships = [
        (membership(vec![(1..=7).collect()], 7, false), 296),
        (membership(vec![(1..=3).collect()], 3, true), 296),
        // Nodes 10 and 11 are learners outside either joint voter set.
        (
            membership(vec![(1..=7).collect(), (3..=9).collect()], 11, true),
            296,
        ),
        // Metadata and full data both exceed the default 8 KiB I/O buffer.
        (membership(vec![(1..=7).collect()], 100, true), 8 * 1024),
    ];
    for (membership, minimum_meta_len) in memberships {
        let source_dir = tempfile::tempdir().unwrap();
        let mut source = store(source_dir.path()).await;
        let expected = SnapshotMeta {
            last_log_id: Some(log_id(30)),
            last_membership: StoredMembership::new(Some(log_id(20)), membership),
        };
        source.update_meta_data(expected.clone(), 99).await;
        source.data.kvs.set_write_clock(1_000);
        source.data.kvs.databases[0].mocha.insert_absolute(
            Bytes::from_static(b"ttl-key"),
            MyValue {
                version: 7,
                data: ValueObject::String(Bytes::from_static(b"snapshot value")),
            },
            5_000,
        );
        source.data.kvs.databases[0].mocha.insert_persistent(
            Bytes::from_static(b"large-key"),
            MyValue::new(ValueObject::String(Bytes::from_static(LARGE_VALUE))),
        );
        let serialized = bincode2::serialize(&CacheCatSnapshotMeta {
            meta: expected.clone(),
            write_clock: 1_000,
            snapshot_revision: 99,
        })
        .unwrap();
        assert!(
            serialized.len() > minimum_meta_len,
            "metadata fixture must exceed {minimum_meta_len} bytes"
        );

        let built = source.build_snapshot().await.unwrap();
        assert_eq!(built.meta, expected);
        assert_eq!(
            built.snapshot.load_meta_data().await.unwrap(),
            Some(expected.clone())
        );
        let current = source.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, expected);

        let file = fs::read(built.snapshot.get_hard_link_buf()).await.unwrap();
        assert_eq!(&file[..5], b"MCDC\x01");
        let offset = u64::from_be_bytes(file[5..13].try_into().unwrap()) as usize;
        let len = u64::from_be_bytes(file[13..21].try_into().unwrap()) as usize;
        assert!(offset >= 21);
        assert_eq!(offset + len, file.len());
        assert_eq!(&file[offset..], serialized);

        let restored = Arc::new(MyCache::new(1).unwrap());
        let (meta, queue, clock, revision) =
            load_cache_from_path(restored.clone(), built.snapshot.get_hard_link_buf())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(meta, expected);
        assert!(queue.is_empty());
        assert_eq!((clock, revision), (1_000, 99));
        assert_value(&restored);

        let mut restarted = store(source_dir.path()).await;
        assert_eq!(
            restarted.applied_state().await.unwrap(),
            (expected.last_log_id, expected.last_membership.clone())
        );
        assert_eq!(restarted.data.kvs.get_write_clock(), 1_000);
        assert_eq!(
            restarted.data.raft_meta_data.lock().await.snapshot_revision,
            99
        );
        assert_value(&restarted.data.kvs);

        let receiver_dir = tempfile::tempdir().unwrap();
        let mut receiver = store(receiver_dir.path()).await;
        fs::create_dir_all(receiver_dir.path().join("snapshot"))
            .await
            .unwrap();
        fs::copy(
            built.snapshot.get_hard_link_buf(),
            built.snapshot.get_local_hard_link_buf(receiver_dir.path()),
        )
        .await
        .unwrap();
        receiver
            .install_snapshot(&built.meta, built.snapshot)
            .await
            .unwrap();
        assert_eq!(
            receiver.applied_state().await.unwrap(),
            (expected.last_log_id, expected.last_membership.clone())
        );
        assert_eq!(receiver.data.kvs.get_write_clock(), 1_000);
        assert_eq!(
            receiver.data.raft_meta_data.lock().await.snapshot_revision,
            99
        );
        assert_value(&receiver.data.kvs);
        assert_eq!(
            receiver.get_current_snapshot().await.unwrap().unwrap().meta,
            expected
        );
        let installed_restart = store(receiver_dir.path()).await;
        assert_value(&installed_restart.data.kvs);
        assert_eq!(
            installed_restart
                .data
                .raft_meta_data
                .lock()
                .await
                .last_membership,
            expected.last_membership
        );
    }
}

#[test]
fn snapshot_captures_membership_and_queue_after_start() {
    // All Tokio filesystem work uses this one blocking worker. The channel
    // makes the first snapshot I/O wait deterministically while Raft applies
    // entries in Start. Dropping the sender also releases the worker on panic.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let path = tempfile::tempdir().unwrap();
        let mut source = store(path.path()).await;
        source.data.kvs.databases[0].mocha.insert_absolute(
            Bytes::from_static(b"persist"),
            MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            500,
        );
        let mut builder = source.clone();
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (started, ready) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            let _ = blocked.recv();
        });
        ready.await.unwrap();

        let mut build = Box::pin(builder.build_snapshot());
        assert!(futures::poll!(build.as_mut()).is_pending());
        assert_eq!(
            source.data.raft_meta_data.lock().await.snapshot_state,
            SnapshotState::Start
        );
        let membership = membership(vec![(1..=7).collect(), (3..=9).collect()], 11, true);
        let stored_membership = StoredMembership::new(Some(log_id(1)), membership.clone());
        source
            .apply(futures::stream::iter([
                Ok((Entry::new_membership(log_id(1), membership), None)),
                Ok((
                    Entry::new_normal(
                        log_id(2),
                        Request::new(
                            400,
                            0,
                            Operation::Base(BaseOperation::Persist(PersistReq {
                                key: Bytes::from_static(b"persist"),
                            })),
                        ),
                    ),
                    None,
                )),
                // This no-op advances the final clock beyond the recorded PERSIST.
                Ok((
                    Entry::new_normal(
                        log_id(3),
                        Request::new(
                            1_000,
                            0,
                            Operation::Base(BaseOperation::Persist(PersistReq {
                                key: Bytes::from_static(b"absent"),
                            })),
                        ),
                    ),
                    None,
                )),
            ]))
            .await
            .unwrap();
        assert_eq!(
            source.data.incremental_operation_queue.lock().await.len(),
            1
        );
        release.send(()).unwrap();
        worker.await.unwrap();

        let built = build.await.unwrap();
        assert_eq!(built.meta.last_membership, stored_membership);
        assert_eq!(built.meta.last_log_id, Some(log_id(3)));
        let restored = Arc::new(MyCache::new(1).unwrap());
        let (meta, queue, clock, revision) =
            load_cache_from_path(restored, built.snapshot.get_hard_link_buf())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(meta, built.meta);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].write_clock, 400);
        assert_eq!(queue[0].version, 1);
        assert_eq!((clock, revision), (1_000, 1));
        assert!(
            source
                .data
                .incremental_operation_queue
                .lock()
                .await
                .is_empty()
        );
        assert_eq!(
            source.data.raft_meta_data.lock().await.snapshot_state,
            SnapshotState::End
        );

        let restarted = store(path.path()).await;
        assert_eq!(restarted.data.kvs.get_write_clock(), 1_000);
        let entry = restarted.data.kvs.databases[0]
            .mocha
            .get_entry(b"persist".as_slice())
            .unwrap();
        assert_eq!(entry.expire_at, None);
        assert_eq!(entry.value.version, 1);
        assert_eq!(
            restarted.data.raft_meta_data.lock().await.last_membership,
            stored_membership
        );
    });
}

#[tokio::test]
async fn snapshot_readers_reject_invalid_metadata_consistently() {
    let path = tempfile::tempdir().unwrap();
    let mut source = store(path.path()).await;
    source
        .update_meta_data(
            SnapshotMeta {
                last_log_id: Some(log_id(2)),
                last_membership: StoredMembership::new(
                    Some(log_id(1)),
                    membership(vec![[1].into_iter().collect()], 1, false),
                ),
            },
            4,
        )
        .await;
    let built = source.build_snapshot().await.unwrap();
    let original = fs::read(built.snapshot.get_hard_link_buf()).await.unwrap();
    let offset = u64::from_be_bytes(original[5..13].try_into().unwrap()) as usize;
    let len = u64::from_be_bytes(original[13..21].try_into().unwrap());
    let mut invalid = Vec::new();
    let mut bytes = original.clone();
    bytes[0] = b'?';
    invalid.push(("magic", bytes));
    let mut bytes = original.clone();
    bytes[4] = 1;
    invalid.push(("old version", bytes));
    for bad_offset in [0, 20, u64::MAX, original.len() as u64 + 1] {
        let mut bytes = original.clone();
        bytes[5..13].copy_from_slice(&bad_offset.to_be_bytes());
        invalid.push(("metadata offset", bytes));
    }
    for bad_len in [0, len - 1, len + 1, u64::MAX] {
        let mut bytes = original.clone();
        bytes[13..21].copy_from_slice(&bad_len.to_be_bytes());
        invalid.push(("metadata length", bytes));
    }
    invalid.push(("truncated header", original[..20].to_vec()));
    invalid.push((
        "truncated metadata",
        original[..original.len() - 1].to_vec(),
    ));
    let mut bytes = original.clone();
    bytes.push(0);
    invalid.push(("data after metadata", bytes.clone()));
    bytes[13..21].copy_from_slice(&(len + 1).to_be_bytes());
    invalid.push(("trailing serialized metadata", bytes));
    let mut bytes = original.clone();
    let watermark_offset = bytes.len() - 8;
    bytes[watermark_offset..].copy_from_slice(&u64::MAX.to_le_bytes());
    invalid.push(("reserved revision", bytes));
    let mut bytes = original.clone();
    let endpoint = b"127.0.0.1";
    let address_offset = offset
        + bytes[offset..]
            .windows(endpoint.len())
            .position(|part| part == endpoint)
            .unwrap();
    bytes[address_offset - 8..address_offset].copy_from_slice(&u64::MAX.to_le_bytes());
    invalid.push(("oversized nested endpoint length", bytes));

    let bad_path = path.path().join("invalid.bin");
    for (case, bytes) in invalid {
        fs::write(&bad_path, bytes).await.unwrap();
        let metadata_error = load_meta_from_path(&bad_path).await.unwrap_err();
        let cache = Arc::new(MyCache::new(1).unwrap());
        cache.databases[0].mocha.insert_persistent(
            Bytes::from_static(b"existing"),
            MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
        );
        let load_error = load_cache_from_path(cache.clone(), &bad_path)
            .await
            .unwrap_err();
        assert_eq!(load_error.kind(), metadata_error.kind(), "{case}");
        assert!(
            cache.databases[0]
                .mocha
                .get(b"existing".as_slice())
                .is_some(),
            "{case}: invalid metadata must be rejected before clearing the cache"
        );
    }
}

#[tokio::test]
async fn snapshot_body_cannot_consume_metadata_or_ignore_trailing_body_bytes() {
    let path = tempfile::tempdir().unwrap();
    dump_cache_to_path(
        Arc::new(MyCache::new(1).unwrap()),
        path.path(),
        Default::default(),
        Default::default(),
    )
    .await
    .unwrap();
    let snapshot_path = path.path().join("snapshot").join(get_snapshot_file_name());
    let original = fs::read(&snapshot_path).await.unwrap();
    let offset = u64::from_be_bytes(original[5..13].try_into().unwrap()) as usize;
    // Empty body: database count, zero entry count, zero queue terminator.
    assert_eq!(offset, 21 + 8 + 8 + 8);
    let mut missing_terminator = original.clone();
    missing_terminator.drain(offset - 8..offset);
    missing_terminator[5..13].copy_from_slice(&((offset - 8) as u64).to_be_bytes());
    let mut trailing_body = original.clone();
    trailing_body.insert(offset, 0);
    trailing_body[5..13].copy_from_slice(&((offset + 1) as u64).to_be_bytes());
    let mut record_overruns_body = original;
    record_overruns_body[offset - 8..offset].copy_from_slice(&8u64.to_be_bytes());
    for bytes in [missing_terminator, trailing_body, record_overruns_body] {
        fs::write(&snapshot_path, bytes).await.unwrap();
        assert!(load_meta_from_path(&snapshot_path).await.unwrap().is_some());
        assert!(
            load_cache_from_path(Arc::new(MyCache::new(1).unwrap()), &snapshot_path)
                .await
                .is_err()
        );
    }
}
