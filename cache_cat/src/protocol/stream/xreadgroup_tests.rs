use super::*;
use crate::mocha::EntrySnapshot;
use crate::protocol::transaction::QueuedOperation;
use crate::protocol::transaction::exec::ExecParams;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::core::response_value::Resp2MapEncoding;
use crate::raft::types::core::structure::stream::{
    AddId, GroupGeneration, GroupStart, ManualClock, RedisStream, StreamId, StreamSnapshot,
};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation};
use crate::utils::OptionalU64;
use parking_lot::RwLock;
use std::sync::Arc;

const DB: u16 = 1;
const CLOCK: u64 = 1_000;
const EXPIRES: u64 = 10_000;

fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

fn items(parts: &[&str]) -> Vec<Value> {
    parts
        .iter()
        .map(|part| Value::BulkString(Some(bytes(part))))
        .collect()
}

fn request(parts: &[&str]) -> Operation {
    XReadGroupCommand.raft_request(&items(parts)).unwrap()
}

fn apply_at(cache: &MyCache, operation: Operation, clock: u64) -> Value {
    let mut update_type = UpdateType::None;
    let mut update = Update {
        execution_id: None,
        db_number: DB,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
        effects: None,
        waiters: None,
    };
    do_request(cache, operation, &mut update, true)
}

fn apply(cache: &MyCache, parts: &[&str]) -> Value {
    apply_at(cache, request(parts), CLOCK)
}

fn seed(cache: &MyCache, key: &str, ids: &[u64], physical_clock: u64) {
    let mut stream = RedisStream::with_clock(Arc::new(ManualClock::new(physical_clock)));
    for &id in ids {
        stream
            .xadd(
                AddId::Explicit(StreamId::new(id, 0)),
                vec![(b"field".to_vec(), id.to_string().into_bytes())],
            )
            .unwrap();
    }
    stream
        .xgroup_create(b"group", GroupStart::Id(StreamId::ZERO), None)
        .unwrap();
    cache.set_write_clock(CLOCK);
    cache.databases[DB as usize].mocha.insert_snapshot(
        bytes(key),
        EntrySnapshot {
            value: MyValue {
                version: 7,
                data: ValueObject::Stream(Arc::new(RwLock::new(stream))),
            },
            expire_at: Some(EXPIRES),
        },
    );
}

fn state(cache: &MyCache, key: &str) -> StreamSnapshot {
    let entry = cache.databases[DB as usize]
        .mocha
        .get_entry(&bytes(key))
        .unwrap();
    let ValueObject::Stream(stream) = entry.value.data else {
        panic!("expected stream")
    };
    let snapshot = stream.read().snapshot();
    snapshot
}

fn response(rows: &[(&str, &[(u64, bool)])]) -> Value {
    Value::MapWithResp2 {
        entries: rows
            .iter()
            .map(|(key, entries)| {
                (
                    Value::BulkString(Some(bytes(key))),
                    Value::Array(Some(
                        entries
                            .iter()
                            .map(|(id, present)| {
                                Value::Array(Some(vec![
                                    Value::BulkString(Some(bytes(&format!("{id}-0")))),
                                    if *present {
                                        Value::Array(Some(vec![
                                            Value::BulkString(Some(bytes("field"))),
                                            Value::BulkString(Some(bytes(&id.to_string()))),
                                        ]))
                                    } else {
                                        Value::Array(None)
                                    },
                                ]))
                            })
                            .collect(),
                    )),
                )
            })
            .collect(),
        resp2: Resp2MapEncoding::Pairs,
    }
}

#[test]
fn parser_keeps_block_timeout_out_of_the_replicated_request() {
    let plain = request(&[
        "XREADGROUP",
        "GROUP",
        "group",
        "consumer",
        "STREAMS",
        "one",
        ">",
    ]);
    for timeout in ["0", "25", "1000"] {
        let blocked = request(&[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "BLOCK",
            timeout,
            "STREAMS",
            "one",
            ">",
        ]);
        assert_eq!(
            bincode2::serialize(&plain).unwrap(),
            bincode2::serialize(&blocked).unwrap()
        );
    }
    assert!(matches!(
        plain,
        Operation::Redis(RedisOperation::RedisXReadGroup(_))
    ));
    for args in [
        vec![
            "XREADGROUP",
            "COUNT",
            "1",
            "BLOCK",
            "0",
            "STREAMS",
            "one",
            ">",
        ],
        vec!["XREADGROUP", "GROUP", "group", "consumer", "STREAMS", "one"],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "two",
            ">",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "BLOCK",
            "-1",
            "STREAMS",
            "one",
            ">",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "BLOCK",
            "1.5",
            "STREAMS",
            "one",
            ">",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "COUNT",
            "01",
            "STREAMS",
            "one",
            ">",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "$",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "+",
        ],
        vec![
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "1-*",
        ],
    ] {
        assert!(
            XReadGroupCommand.raft_request(&items(&args)).is_err(),
            "accepted {args:?}"
        );
    }
    // Redis accepts options in any order and treats nonpositive COUNT as unlimited.
    assert!(
        XReadGroupCommand
            .raft_request(&items(&[
                "XREADGROUP",
                "COUNT",
                "-1",
                "GROUP",
                "group",
                "consumer",
                "STREAMS",
                "one",
                ">"
            ]))
            .is_ok()
    );
}

#[test]
fn count_applies_per_stream_and_duplicate_keys_consume_in_argument_order() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "one", &[1, 2], 0);
    seed(&cache, "two", &[3, 4], u64::MAX);
    let result = apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "COUNT",
            "1",
            "STREAMS",
            "one",
            "two",
            "one",
            ">",
            ">",
            ">",
        ],
    );
    let expected = response(&[
        ("one", &[(1, true)]),
        ("two", &[(3, true)]),
        ("one", &[(2, true)]),
    ]);
    assert_eq!(result.encode_proto(2), expected.encode_proto(2));
    assert_eq!(result.encode_proto(3), expected.encode_proto(3));
    assert_eq!(state(&cache, "one").groups[0].pending.len(), 2);
    assert_eq!(state(&cache, "two").groups[0].pending.len(), 1);
    assert_eq!(state(&cache, "one").groups[0].consumers[0].seen_ms, CLOCK);
    assert_eq!(
        cache.databases[DB as usize]
            .mocha
            .get_entry(&bytes("one"))
            .unwrap()
            .expire_at,
        Some(EXPIRES)
    );
    assert_eq!(cache.databases[0].mocha.len(), 0);
}

#[test]
fn all_keys_and_groups_are_validated_before_any_consumer_or_pel_is_changed() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "one", &[1], 0);
    let before = state(&cache, "one");
    let missing = apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "absent",
            ">",
            ">",
        ],
    );
    assert!(matches!(missing, Value::Error(error) if error.starts_with("NOGROUP")));
    assert_eq!(state(&cache, "one"), before);
    cache.databases[DB as usize].mocha.insert_persistent(
        bytes("wrong"),
        MyValue::new(ValueObject::String(bytes("value"))),
    );
    let wrong = apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            "wrong",
            ">",
            ">",
        ],
    );
    assert!(matches!(wrong, Value::Error(error) if error.starts_with("WRONGTYPE")));
    assert_eq!(state(&cache, "one"), before);
}

#[test]
fn empty_new_read_still_creates_consumer_and_noack_only_skips_pending_records() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "empty", &[], u64::MAX);
    let empty = apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "empty",
            ">",
        ],
    );
    assert_eq!(empty.encode(), b"*-1\r\n");
    let snapshot = state(&cache, "empty");
    assert_eq!(snapshot.groups[0].consumers.len(), 1);
    assert_eq!(snapshot.groups[0].consumers[0].seen_ms, CLOCK);
    assert!(snapshot.groups[0].pending.is_empty());
    seed(&cache, "one", &[1, 2], u64::MAX);
    let noack = apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "COUNT",
            "0",
            "NOACK",
            "STREAMS",
            "one",
            ">",
        ],
    );
    assert_eq!(
        noack.encode(),
        response(&[("one", &[(1, true), (2, true)])]).encode()
    );
    let snapshot = state(&cache, "one");
    assert!(snapshot.groups[0].pending.is_empty());
    assert_eq!(snapshot.groups[0].last_delivered, StreamId::new(2, 0));
    assert_eq!(snapshot.groups[0].consumers[0].seen_ms, CLOCK);
    assert_eq!(snapshot.groups[0].consumers[0].active_ms, None);
}

#[test]
fn history_returns_owned_pending_and_deleted_payloads_and_mixed_empty_rows() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "one", &[1, 2], u64::MAX);
    seed(&cache, "empty", &[], u64::MAX);
    apply(
        &cache,
        &[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "STREAMS",
            "one",
            ">",
        ],
    );
    let entry = cache.databases[DB as usize]
        .mocha
        .get_entry(&bytes("one"))
        .unwrap();
    let ValueObject::Stream(stream) = entry.value.data else {
        unreachable!()
    };
    stream.write().xdel(&[StreamId::new(1, 0)]);
    let history = apply_at(
        &cache,
        request(&[
            "XREADGROUP",
            "GROUP",
            "group",
            "consumer",
            "NOACK",
            "BLOCK",
            "0",
            "STREAMS",
            "one",
            "empty",
            "0",
            "0",
        ]),
        CLOCK + 100,
    );
    assert_eq!(
        history.encode(),
        response(&[("one", &[(1, false), (2, true)]), ("empty", &[])]).encode()
    );
    let snapshot = state(&cache, "one");
    assert_eq!(snapshot.groups[0].pending[0].deliveries, 1);
    assert_eq!(snapshot.groups[0].pending[1].deliveries, 2);
    assert_eq!(snapshot.groups[0].pending[1].delivery_ms, CLOCK + 100);
    assert_eq!(snapshot.groups[0].consumers[0].active_ms, Some(CLOCK));
    let other = apply_at(
        &cache,
        request(&[
            "XREADGROUP",
            "GROUP",
            "group",
            "other",
            "STREAMS",
            "one",
            "empty",
            "0",
            ">",
        ]),
        CLOCK + 100,
    );
    assert_eq!(other.encode(), response(&[("one", &[])]).encode());
}

fn snapshot_apply(cache: &MyCache, operation: Operation) -> (Value, Vec<AtomicRequest>) {
    let mut queue = Vec::new();
    let mut revision = 7;
    let mut update_type = UpdateType::Snapshot {
        queue: &mut queue,
        revision: &mut revision,
    };
    let mut update = Update {
        execution_id: None,
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
        effects: None,
        waiters: None,
    };
    let result = do_request(cache, operation, &mut update, true);
    (result, queue)
}

fn replay(cache: &MyCache, queue: &[AtomicRequest]) {
    let encoded = bincode2::serialize(queue).unwrap();
    let queue: Vec<AtomicRequest> = bincode2::deserialize(&encoded).unwrap();
    for atomic in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: atomic.expected_revision,
            revision: atomic.version,
        };
        let mut update = Update {
            execution_id: None,
            db_number: atomic.db_number,
            write_clock: cache.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        base_request(cache, atomic.request, &mut update);
    }
}

#[test]
fn snapshot_replays_single_stream_effects_with_ttl_and_deterministic_delivery_time() {
    let live = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    for cache in [&live, &restored] {
        seed(cache, "one", &[1, 2], 0);
        seed(cache, "two", &[3, 4], u64::MAX);
    }
    let operation = request(&[
        "XREADGROUP",
        "GROUP",
        "group",
        "consumer",
        "COUNT",
        "1",
        "STREAMS",
        "one",
        "two",
        ">",
        ">",
    ]);
    let (reply, queue) = snapshot_apply(&live, operation);
    assert_eq!(
        reply.encode(),
        response(&[("one", &[(1, true)]), ("two", &[(3, true)])]).encode()
    );
    assert_eq!(queue.len(), 2);
    for atomic in &queue {
        assert!(matches!(&atomic.request, BaseOperation::XReadGroup(_)));
        assert_eq!(atomic.expected_revision, OptionalU64::some(7));
        assert_eq!(atomic.db_number, DB);
        assert_eq!(atomic.write_clock, CLOCK);
        assert!(atomic.version > 7);
    }
    let one = state(&live, "one");
    let two = state(&live, "two");
    apply_at(
        &live,
        request(&[
            "XREADGROUP",
            "GROUP",
            "group",
            "later",
            "STREAMS",
            "one",
            "two",
            ">",
            ">",
        ]),
        CLOCK + 100,
    );
    replay(&restored, &queue);
    replay(&restored, &queue);
    assert_eq!(state(&restored, "one"), one);
    assert_eq!(state(&restored, "two"), two);
    assert_eq!(
        restored.databases[DB as usize]
            .mocha
            .get_entry(&bytes("one"))
            .unwrap()
            .expire_at,
        Some(EXPIRES)
    );
    assert_eq!(
        restored.databases[DB as usize]
            .mocha
            .get_entry(&bytes("two"))
            .unwrap()
            .expire_at,
        Some(EXPIRES)
    );
}

#[test]
fn empty_group_reads_are_snapshot_writes_and_exec_never_registers_waiters() {
    let live = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    for cache in [&live, &restored] {
        seed(cache, "empty", &[], u64::MAX);
    }
    let operation = request(&[
        "XREADGROUP",
        "GROUP",
        "group",
        "consumer",
        "BLOCK",
        "0",
        "STREAMS",
        "empty",
        ">",
    ]);
    let exec = Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: vec![QueuedOperation::new(DB, operation)],
    }));
    let (reply, queue) = snapshot_apply(&live, exec);
    assert_eq!(reply.encode(), b"*1\r\n*-1\r\n");
    assert_eq!(queue.len(), 1);
    replay(&restored, &queue);
    assert_eq!(state(&live, "empty"), state(&restored, "empty"));
    assert_eq!(
        state(&restored, "empty").groups[0].consumers[0].seen_ms,
        CLOCK
    );
}

#[test]
fn binary_stream_and_consumer_names_are_preserved_in_reply_and_pending_state() {
    let cache = MyCache::new(2).unwrap();
    let key = Bytes::from_static(b"\xff\0stream");
    let consumer = Bytes::from_static(b"\0\xffconsumer");
    let group = Bytes::from_static(b"\xff\0group");
    let mut stream = RedisStream::with_clock(Arc::new(ManualClock::new(u64::MAX)));
    stream
        .xadd(
            AddId::Explicit(StreamId::new(1, 0)),
            vec![(b"field".to_vec(), b"1".to_vec())],
        )
        .unwrap();
    stream
        .xgroup_create(&group, GroupStart::Id(StreamId::ZERO), None)
        .unwrap();
    cache.set_write_clock(CLOCK);
    cache.databases[DB as usize].mocha.insert_persistent(
        key.clone(),
        MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream)))),
    );
    let input = vec![
        Value::BulkString(Some(bytes("XREADGROUP"))),
        Value::BulkString(Some(bytes("GROUP"))),
        Value::BulkString(Some(group.clone())),
        Value::BulkString(Some(consumer.clone())),
        Value::BulkString(Some(bytes("STREAMS"))),
        Value::BulkString(Some(key.clone())),
        Value::BulkString(Some(bytes(">"))),
    ];
    let reply = apply_at(
        &cache,
        XReadGroupCommand.raft_request(&input).unwrap(),
        CLOCK,
    );
    let Value::MapWithResp2 { entries, .. } = reply else {
        panic!("expected stream reply")
    };
    assert_eq!(entries[0].0.string_bytes_clone(), Some(key.clone()));
    let entry = cache.databases[DB as usize].mocha.get_entry(&key).unwrap();
    let ValueObject::Stream(stream) = entry.value.data else {
        panic!("expected stream")
    };
    let snapshot = stream.read().snapshot();
    assert_eq!(snapshot.groups[0].name, group.as_ref());
    assert_eq!(snapshot.groups[0].pending[0].consumer, consumer.as_ref());
    assert_eq!(snapshot.groups[0].consumers[0].name, consumer.as_ref());
}

#[test]
fn group_recreation_in_one_exec_has_distinct_generations_and_snapshot_replays_them() {
    use crate::protocol::stream::xgroup::{XGroupCommand, XGroupReq};
    let live = MyCache::new(2).unwrap();
    let restored = MyCache::new(2).unwrap();
    let create = || {
        XGroupCommand
            .raft_request(&items(&[
                "XGROUP", "CREATE", "stream", "group", "0", "MKSTREAM",
            ]))
            .unwrap()
    };
    let destroy = XGroupCommand
        .raft_request(&items(&["XGROUP", "DESTROY", "stream", "group"]))
        .unwrap();
    let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: vec![
            QueuedOperation::new(DB, create()),
            QueuedOperation::new(DB, destroy),
            QueuedOperation::new(DB, create()),
        ],
    }));
    let mut queue = Vec::new();
    let mut revision = 0;
    let mut update_type = UpdateType::Snapshot {
        queue: &mut queue,
        revision: &mut revision,
    };
    let mut update = Update {
        db_number: DB,
        write_clock: live.set_write_clock(CLOCK),
        update_type: &mut update_type,
        effects: None,
        waiters: None,
        execution_id: Some((42, 0)),
    };
    let reply = do_request(&live, operation, &mut update, true);
    assert_eq!(reply.encode(), b"*3\r\n+OK\r\n:1\r\n+OK\r\n");
    assert_eq!(update.execution_id, Some((42, 2)));
    let generations: Vec<_> = queue
        .iter()
        .filter_map(|atomic| match &atomic.request {
            BaseOperation::XGroup(XGroupReq::Create { generation, .. }) => *generation,
            _ => None,
        })
        .collect();
    assert_eq!(
        generations,
        vec![
            GroupGeneration {
                log_index: 42,
                ordinal: 1
            },
            GroupGeneration {
                log_index: 42,
                ordinal: 2
            }
        ]
    );
    assert_eq!(
        state(&live, "stream").groups[0].generation,
        Some(generations[1])
    );
    replay(&restored, &queue);
    replay(&restored, &queue);
    assert_eq!(state(&live, "stream"), state(&restored, "stream"));
}

#[test]
fn queued_group_read_rejects_recreated_generation_before_mutating_any_stream() {
    use crate::protocol::stream::xadd::XAddCommand;
    use crate::protocol::stream::xgroup::XGroupCommand;
    let cache = MyCache::new(2).unwrap();
    let apply_log = |operation: Operation, log_index: u64| {
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: DB,
            write_clock: cache.set_write_clock(CLOCK),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
            execution_id: Some((log_index, 0)),
        };
        do_request(&cache, operation, &mut update, true)
    };
    let create = |key: &str| {
        XGroupCommand
            .raft_request(&items(&["XGROUP", "CREATE", key, "group", "0", "MKSTREAM"]))
            .unwrap()
    };
    let append = |key: &str, id: &str| {
        XAddCommand
            .raft_request(&items(&["XADD", key, id, "field", "value"]))
            .unwrap()
    };
    for (key, index) in [("valid", 10), ("recreated", 11)] {
        assert_eq!(apply_log(create(key), index).encode(), b"+OK\r\n");
        apply_log(append(key, "1-0"), index + 2);
    }
    let Operation::Redis(RedisOperation::RedisXReadGroup(mut queued)) = request(&[
        "XREADGROUP",
        "GROUP",
        "group",
        "old-consumer",
        "STREAMS",
        "valid",
        "recreated",
        ">",
        ">",
    ]) else {
        unreachable!()
    };
    queued.expected_generations = vec![
        state(&cache, "valid").groups[0].generation,
        state(&cache, "recreated").groups[0].generation,
    ];
    let old_generation = queued.expected_generations[1];
    let serialized = bincode2::serialize(&queued).unwrap();
    let queued: XReadGroupReq = bincode2::deserialize(&serialized).unwrap();

    // The earlier local check has finished, but other committed operations
    // destroy/recreate the group before this read reaches the state machine.
    apply_log(
        XGroupCommand
            .raft_request(&items(&["XGROUP", "DESTROY", "recreated", "group"]))
            .unwrap(),
        20,
    );
    assert_eq!(apply_log(create("recreated"), 21).encode(), b"+OK\r\n");
    apply_log(append("recreated", "2-0"), 22);
    let valid_before = state(&cache, "valid");
    let recreated_before = state(&cache, "recreated");
    assert_ne!(recreated_before.groups[0].generation, old_generation);

    let mut single = queued.clone();
    single.keys.remove(0);
    single.modes.remove(0);
    single.expected_generations.remove(0);
    for pending in [single, queued] {
        let (reply, incremental) = snapshot_apply(
            &cache,
            Operation::Redis(RedisOperation::RedisXReadGroup(pending)),
        );
        assert!(matches!(reply, Value::Error(error) if error.starts_with("NOGROUP")));
        assert!(incremental.is_empty());
        assert_eq!(state(&cache, "valid"), valid_before);
        assert_eq!(state(&cache, "recreated"), recreated_before);
    }
}
