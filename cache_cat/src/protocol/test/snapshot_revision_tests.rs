use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::key::flushall::FlushAllReq;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::transaction::QueuedOperation;
use crate::protocol::transaction::exec::ExecParams;
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation};
use crate::utils::OptionalU64;
use bytes::Bytes;
use std::io::Cursor;

fn command(args: &[&str]) -> Operation {
    // The Lua factory does not register every command accepted by the normal
    // protocol dispatcher. Exercise their existing state-machine requests here.
    match args[0] {
        "FLUSHALL" => {
            return Operation::Base(BaseOperation::FlushAll(FlushAllReq { async_mode: false }));
        }
        "HDEL" => {
            return Operation::Base(BaseOperation::HDel(HDelReq {
                key: Bytes::copy_from_slice(args[1].as_bytes()),
                fields: args[2..]
                    .iter()
                    .map(|field| Bytes::copy_from_slice(field.as_bytes()))
                    .collect(),
            }));
        }
        _ => {}
    }
    let values = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua()
        .parse_request(&values)
        .unwrap()
}

fn apply(
    cache: &MyCache,
    operation: Operation,
    db_number: u16,
    clock: u64,
    snapshot: Option<(&mut Vec<AtomicRequest>, &mut u64)>,
) -> Value {
    let mut update_type = match snapshot {
        Some((queue, revision)) => UpdateType::Snapshot { queue, revision },
        None => UpdateType::None,
    };
    let mut update = Update {
        db_number,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
    };
    do_request(cache, operation, &mut update, true)
}

async fn dump(cache: &MyCache) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    cache.dump_cache_to_writer(&mut bytes).await.unwrap();
    bytes.into_inner()
}

async fn restore(full: &[u8], queue: &[AtomicRequest], final_clock: u64) -> MyCache {
    let cache = MyCache::new(2).unwrap();
    cache.pause_expire_workers();
    cache
        .load_cache_from_reader(&mut Cursor::new(full))
        .await
        .unwrap();
    let encoded = bincode2::serialize(queue).unwrap();
    let persisted_queue: Vec<AtomicRequest> = bincode2::deserialize(&encoded).unwrap();
    for delta in persisted_queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: delta.expected_revision,
            revision: delta.version,
        };
        let mut update = Update {
            db_number: delta.db_number,
            write_clock: cache.set_write_clock(delta.write_clock),
            update_type: &mut update_type,
        };
        base_request(&cache, delta.request, &mut update);
    }
    cache.set_write_clock(final_clock);
    cache.resume_expire_workers();
    cache
}

#[derive(Debug, PartialEq, Eq)]
enum LogicalValue {
    String(Bytes),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    Hash(Vec<(Bytes, Bytes)>),
}

fn entry(cache: &MyCache, db: usize, key: &str) -> Option<(Option<u64>, LogicalValue)> {
    let entry = cache.databases[db].mocha.get_entry(key.as_bytes())?;
    let value = match entry.value.data {
        ValueObject::String(value) => LogicalValue::String(value),
        ValueObject::Int(value) => LogicalValue::String(value.to_string().into()),
        ValueObject::List(value) => LogicalValue::List(value.lock().iter().cloned().collect()),
        ValueObject::Set(value) => {
            let mut members = value.lock().iter().cloned().collect::<Vec<_>>();
            members.sort();
            LogicalValue::Set(members)
        }
        ValueObject::Hash(value) => {
            let mut fields = value
                .lock()
                .iter()
                .map(|(key, value)| (key.clone(), value.to_bytes()))
                .collect::<Vec<_>>();
            fields.sort();
            LogicalValue::Hash(fields)
        }
        other => panic!("unexpected test value {other:?}"),
    };
    Some((entry.expire_at, value))
}

async fn assert_all_scan_cuts(
    cache: &MyCache,
    mut revision: u64,
    operations: &[(u16, u64, Operation)],
    keys: &[&str],
) {
    let mut scans = vec![dump(cache).await];
    let mut queue = Vec::new();
    for (db, clock, operation) in operations {
        let response = apply(
            cache,
            operation.clone(),
            *db,
            *clock,
            Some((&mut queue, &mut revision)),
        );
        assert!(!matches!(response, Value::Error(_)), "{response:?}");
        scans.push(dump(cache).await);
    }
    assert!(
        queue
            .windows(2)
            .all(|pair| pair[0].version < pair[1].version)
    );
    let final_clock = cache.get_write_clock();
    for (cut, full) in scans.iter().enumerate() {
        let restored = restore(full, &queue, final_clock).await;
        for db in 0..2 {
            for key in keys {
                assert_eq!(
                    entry(&restored, db, key),
                    entry(cache, db, key),
                    "scan after operation {cut}, db {db}, key {key}"
                );
            }
        }
    }
}

#[tokio::test]
async fn delete_and_recreate_does_not_replay_old_increment_on_new_value() {
    let cache = MyCache::new(2).unwrap();
    apply(
        &cache,
        command(&["SET", "k", "0", "PX", "100000"]),
        0,
        1000,
        None,
    );
    let operations = [
        (0, 1000, command(&["INCR", "k"])),
        (0, 1000, command(&["PERSIST", "k"])),
        (0, 1000, command(&["DEL", "k"])),
        (0, 1000, command(&["SET", "k", "10"])),
    ];
    assert_all_scan_cuts(&cache, 0, &operations, &["k"]).await;
    assert_eq!(
        entry(&cache, 0, "k"),
        Some((None, LogicalValue::String("10".into())))
    );
}

#[tokio::test]
async fn expired_full_scan_entry_is_not_recreated_by_an_old_rmw() {
    let cache = MyCache::new(2).unwrap();
    apply(
        &cache,
        command(&["SET", "k", "10", "PX", "100"]),
        0,
        10,
        None,
    );
    let operations = [
        (0, 50, command(&["INCR", "k"])),
        (1, 200, command(&["SET", "clock", "advanced"])),
    ];
    assert_all_scan_cuts(&cache, 0, &operations, &["k", "clock"]).await;
    assert_eq!(entry(&cache, 0, "k"), None);
}

#[tokio::test]
async fn persist_and_expiration_recreation_keep_the_original_ttl_semantics() {
    let cache = MyCache::new(2).unwrap();
    apply(
        &cache,
        command(&["SET", "persisted", "10", "PX", "100"]),
        0,
        10,
        None,
    );
    apply(
        &cache,
        command(&["SET", "recreated", "10", "PX", "100"]),
        0,
        10,
        None,
    );
    let operations = [
        (0, 50, command(&["INCR", "persisted"])),
        (0, 60, command(&["PERSIST", "persisted"])),
        (0, 200, command(&["INCR", "recreated"])),
        (0, 210, command(&["PEXPIRE", "recreated", "500"])),
    ];
    assert_all_scan_cuts(&cache, 0, &operations, &["persisted", "recreated"]).await;
    assert_eq!(
        entry(&cache, 0, "persisted"),
        Some((None, LogicalValue::String("11".into())))
    );
    assert_eq!(
        entry(&cache, 0, "recreated"),
        Some((Some(710), LogicalValue::String("1".into())))
    );
}

#[tokio::test]
async fn repeated_removals_and_cross_type_recreations_match_every_scan_cut() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, command(&["SET", "k", "old"]), 0, 1, None);
    let commands = [
        vec!["DEL", "k"],
        vec!["SADD", "k", "a", "b"],
        vec!["SREM", "k", "a", "b"],
        vec!["LPUSH", "k", "left"],
        vec!["RPOP", "k"],
        vec!["HSET", "k", "field", "value"],
        vec!["HDEL", "k", "field"],
        vec!["INCR", "k"],
        vec!["UNLINK", "k"],
        vec!["SET", "k", "10", "PX", "1000"],
        vec!["INCR", "k"],
        vec!["DEL", "k"],
        vec!["SADD", "k", "final", "members"],
    ];
    let operations = commands
        .iter()
        .enumerate()
        .map(|(index, args)| (0, index as u64 + 2, command(args)))
        .collect::<Vec<_>>();
    assert_all_scan_cuts(&cache, 0, &operations, &["k"]).await;
}

#[tokio::test]
async fn flush_replay_preserves_later_writes_and_database_scope() {
    for flush in ["FLUSHDB", "FLUSHALL"] {
        let cache = MyCache::new(2).unwrap();
        for db in 0..2 {
            apply(&cache, command(&["SET", "old", "10"]), db, 1, None);
        }
        let operations = [
            (0, 2, command(&["INCR", "old"])),
            (0, 3, command(&[flush])),
            (0, 4, command(&["SET", "old", "20"])),
            (0, 5, command(&["INCR", "old"])),
            (1, 6, command(&["SET", "after", "other-db"])),
            (0, 7, command(&["SADD", "after", "member"])),
        ];
        assert_all_scan_cuts(&cache, 0, &operations, &["old", "after"]).await;
    }
}

#[tokio::test]
async fn exec_and_lua_writes_to_the_same_key_have_distinct_revisions() {
    let commands = [
        vec!["INCR", "k"],
        vec!["PERSIST", "k"],
        vec!["DEL", "k"],
        vec!["SET", "k", "10"],
    ];
    let exec = Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: commands
            .iter()
            .map(|args| QueuedOperation::new(0, command(args)))
            .collect(),
    }));
    let eval = Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
        "redis.call('INCR', 'k'); redis.call('PERSIST', 'k'); redis.call('DEL', 'k'); return redis.call('SET', 'k', '10')".into(),
        0, vec![], vec![],
    )));
    for operation in [exec, eval] {
        let cache = MyCache::new(2).unwrap();
        apply(
            &cache,
            command(&["SET", "k", "0", "PX", "100000"]),
            0,
            1000,
            None,
        );
        assert_all_scan_cuts(&cache, 0, &[(0, 1000, operation)], &["k"]).await;
        assert_eq!(
            entry(&cache, 0, "k"),
            Some((None, LogicalValue::String("10".into())))
        );
    }
}

#[tokio::test]
async fn restored_revisions_do_not_collide_in_the_next_snapshot() {
    let cache = MyCache::new(2).unwrap();
    let full = dump(&cache).await;
    let mut queue = Vec::new();
    let mut revision = 0;
    for args in [
        vec!["SET", "k", "5"],
        vec!["SET", "removed", "x"],
        vec!["DEL", "removed"],
    ] {
        apply(
            &cache,
            command(&args),
            0,
            1,
            Some((&mut queue, &mut revision)),
        );
    }
    let watermark = revision;
    let restored = restore(&full, &queue, 1).await;
    // Between snapshots normal writes reset only the touched entry to revision 0.
    apply(&restored, command(&["SET", "normal", "7"]), 1, 2, None);
    let operations = [
        (0, 3, command(&["INCR", "k"])),
        (1, 4, command(&["INCR", "normal"])),
        (0, 5, command(&["DEL", "k"])),
        (0, 6, command(&["INCR", "k"])),
    ];
    // The state machine restores this watermark from the snapshot metadata.
    assert_all_scan_cuts(
        &restored,
        watermark,
        &operations,
        &["k", "normal", "removed"],
    )
    .await;
    assert!(
        restored.databases[0]
            .mocha
            .get_entry(b"k".as_slice())
            .unwrap()
            .value
            .version
            > watermark
    );
}

#[test]
fn different_local_snapshot_revisions_do_not_change_replicated_command_results() {
    let ordinary = MyCache::new(2).unwrap();
    let snapshotting = MyCache::new(2).unwrap();
    let mut queue = Vec::new();
    let mut revision = 0;
    let mut operations = [
        vec!["SET", "k", "10", "PX", "500"],
        vec!["INCR", "k"],
        vec!["PERSIST", "k"],
        vec!["SADD", "left", "a", "b"],
        vec!["SADD", "right", "b", "c"],
        vec!["SUNIONSTORE", "dest", "left", "right"],
        vec!["RENAME", "dest", "renamed"],
        vec!["DEL", "k"],
        vec!["SET", "k", "20"],
    ]
    .iter()
    .map(|args| command(args))
    .collect::<Vec<_>>();
    operations.push(Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: vec![
            QueuedOperation::new(0, command(&["INCR", "k"])),
            QueuedOperation::new(1, command(&["SET", "k", "other-db", "PX", "50"])),
            QueuedOperation::new(0, command(&["HSET", "hash", "field", "value"])),
        ],
    })));
    operations.push(Operation::Redis(RedisOperation::RedisEval(
        EvalParams::new(
            "redis.call('INCR', 'k'); return redis.call('GET', 'k')".into(),
            0,
            vec![],
            vec![],
        ),
    )));
    operations.push(command(&["SET", "clock", "advanced"]));
    for (index, operation) in operations.into_iter().enumerate() {
        // Both replicas consume the same persisted Raft operation. Only one is
        // taking a local snapshot, so its local revisions intentionally differ.
        let encoded = bincode2::serialize(&operation).unwrap();
        let replica_operation: Operation = bincode2::deserialize(&encoded).unwrap();
        let clock = (index as u64 + 1) * 100;
        let ordinary_reply = apply(&ordinary, operation, 0, clock, None);
        let snapshot_reply = apply(
            &snapshotting,
            replica_operation,
            0,
            clock,
            Some((&mut queue, &mut revision)),
        );
        assert_eq!(
            ordinary_reply.encode(),
            snapshot_reply.encode(),
            "operation {index}"
        );
        for db in 0..2 {
            for key in ["k", "left", "right", "dest", "renamed", "hash", "clock"] {
                assert_eq!(
                    entry(&ordinary, db, key),
                    entry(&snapshotting, db, key),
                    "operation {index}, db {db}, key {key}"
                );
            }
        }
    }
    assert!(revision > 0);
}

#[test]
fn normal_writes_and_snapshot_noops_do_not_allocate_revisions() {
    let cache = MyCache::new(2).unwrap();
    for _ in 0..32 {
        apply(&cache, command(&["SET", "k", "value"]), 0, 1, None);
    }
    assert_eq!(
        cache.databases[0]
            .mocha
            .get_entry(b"k".as_slice())
            .unwrap()
            .value
            .version,
        0
    );
    let mut queue = Vec::new();
    let mut revision = 0;
    for args in [
        vec!["SET", "k", "other", "NX"],
        vec!["PERSIST", "k"],
        vec!["INCR", "k"],
    ] {
        apply(
            &cache,
            command(&args),
            0,
            1,
            Some((&mut queue, &mut revision)),
        );
    }
    assert!(queue.is_empty());
    assert_eq!(revision, 0);
}

#[test]
fn optional_revision_has_an_eight_byte_persisted_representation() {
    assert_eq!(std::mem::size_of::<OptionalU64>(), 8);
    for revision in [
        OptionalU64::NONE,
        OptionalU64::some(0),
        OptionalU64::some(u64::MAX - 1),
    ] {
        let encoded = bincode2::serialize(&revision).unwrap();
        assert_eq!(encoded.len(), 8);
        assert_eq!(
            bincode2::deserialize::<OptionalU64>(&encoded).unwrap(),
            revision
        );
    }
}
