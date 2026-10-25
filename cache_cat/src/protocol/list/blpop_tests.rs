use super::*;
use crate::mocha::ExpirePolicy;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::transaction::QueuedOperation;
use crate::protocol::transaction::exec::ExecParams;
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation};
use crate::utils::OptionalU64;
use std::collections::VecDeque;

const DB: u16 = 1;
const CLOCK: u64 = 1_000;
const EXPIRES_AT: u64 = 10_000;

fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

fn request(keys: &[&str], side: PopSide) -> BlPopReq {
    BlPopReq {
        keys: keys.iter().map(|key| bytes(key)).collect(),
        side,
    }
}

fn list(values: &[&str]) -> EntrySnapshot<MyValue> {
    EntrySnapshot {
        value: MyValue {
            version: 7,
            data: ValueObject::List(Arc::new(Mutex::new(
                values.iter().map(|value| bytes(value)).collect(),
            ))),
        },
        expire_at: Some(EXPIRES_AT),
    }
}

fn wrong_type() -> EntrySnapshot<MyValue> {
    EntrySnapshot {
        value: MyValue::new(ValueObject::String(bytes("not a list"))),
        expire_at: None,
    }
}

fn elements(value: &MyValue) -> VecDeque<Bytes> {
    let ValueObject::List(list) = &value.data else {
        panic!("expected a list");
    };
    list.lock().clone()
}

fn reply(key: &str, element: &str) -> Vec<u8> {
    Value::Array(Some(vec![
        Value::BulkString(Some(bytes(key))),
        Value::BulkString(Some(bytes(element))),
    ]))
    .encode()
    .to_vec()
}

#[test]
fn first_available_key_wins_without_mutating_input_or_checking_later_types() {
    let selected = list(&["first", "second"]);
    let (writes, result) = request(&["missing", "empty", "selected", "wrong"], PopSide::Left)
        .mutate_writes(
            vec![
                None,
                Some(list(&[])),
                Some(selected.clone()),
                Some(wrong_type()),
            ],
            CLOCK,
        );
    assert_eq!(result.encode(), reply("selected", "first"));
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].key, bytes("selected"));
    let MochaOperation::Insert { value, expire } = &writes[0].operation else {
        panic!("expected the remaining list");
    };
    assert!(matches!(expire, ExpirePolicy::Absolute(EXPIRES_AT)));
    assert_eq!(elements(value), VecDeque::from([bytes("second")]));
    assert_eq!(
        elements(&selected.value),
        VecDeque::from([bytes("first"), bytes("second")])
    );
}

#[test]
fn wrong_type_before_the_first_available_list_returns_error_without_writes() {
    let selected = list(&["untouched"]);
    let (writes, result) = request(&["missing", "wrong", "selected"], PopSide::Left).mutate_writes(
        vec![None, Some(wrong_type()), Some(selected.clone())],
        CLOCK,
    );
    assert!(writes.is_empty());
    assert!(matches!(result, Value::Error(error) if error.starts_with("WRONGTYPE")));
    assert_eq!(
        elements(&selected.value),
        VecDeque::from([bytes("untouched")])
    );
}

#[test]
fn empty_lists_are_noops_and_final_pop_removes_only_selected_key() {
    let (writes, result) = request(&["missing", "empty"], PopSide::Left)
        .mutate_writes(vec![None, Some(list(&[]))], CLOCK);
    assert!(writes.is_empty());
    assert_eq!(result.encode(), b"*-1\r\n");

    let entry = list(&["one"]);
    let (writes, result) = request(&["same", "same"], PopSide::Left)
        .mutate_writes(vec![Some(entry.clone()), Some(entry.clone())], CLOCK);
    assert_eq!(result.encode(), reply("same", "one"));
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].key, bytes("same"));
    assert!(matches!(writes[0].operation, MochaOperation::Remove));
    assert_eq!(elements(&entry.value), VecDeque::from([bytes("one")]));
}

#[test]
fn pop_side_can_reuse_the_same_deterministic_operation_for_brpop() {
    let (writes, result) = request(&["queue"], PopSide::Right)
        .mutate_writes(vec![Some(list(&["first", "last"]))], CLOCK);
    assert_eq!(result.encode(), reply("queue", "last"));
    let MochaOperation::Insert { value, .. } = &writes[0].operation else {
        panic!("expected the remaining list");
    };
    assert_eq!(elements(value), VecDeque::from([bytes("first")]));
}

fn apply(cache: &MyCache, operation: Operation) -> Value {
    let mut update_type = UpdateType::None;
    let mut update = Update {
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    do_request(cache, operation, &mut update, true)
}

fn seed(cache: &MyCache, key: &str, entry: EntrySnapshot<MyValue>) {
    cache.set_write_clock(CLOCK);
    cache.databases[DB as usize]
        .mocha
        .insert_snapshot(bytes(key), entry);
}

fn snapshot(cache: &MyCache, request: BlPopReq) -> (Value, Vec<AtomicRequest>) {
    let mut queue = Vec::new();
    let mut revision = 7;
    let mut update_type = UpdateType::Snapshot {
        queue: &mut queue,
        revision: &mut revision,
    };
    let mut update = Update {
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    let result = do_request(
        cache,
        Operation::Redis(RedisOperation::RedisBlPop(request)),
        &mut update,
        true,
    );
    (result, queue)
}

fn replay(cache: &MyCache, queue: &[AtomicRequest]) {
    let serialized = bincode2::serialize(queue).unwrap();
    let queue: Vec<AtomicRequest> = bincode2::deserialize(&serialized).unwrap();
    for atomic in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: atomic.expected_revision,
            revision: atomic.version,
        };
        let mut update = Update {
            db_number: atomic.db_number,
            write_clock: cache.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
        };
        base_request(cache, atomic.request, &mut update);
    }
}

#[test]
fn snapshot_records_frozen_insert_and_restore_does_not_reselect_a_key() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "chosen", list(&["a", "b"]));
    let (result, queue) = snapshot(&cache, request(&["earlier", "chosen"], PopSide::Left));
    assert_eq!(result.encode(), reply("chosen", "a"));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].expected_revision, OptionalU64::some(7));
    assert_eq!(queue[0].db_number, DB);
    assert_eq!(queue[0].write_clock, CLOCK);
    assert!(queue[0].version > 7);
    let BaseOperation::Insert(insert) = &queue[0].request else {
        panic!("snapshot must record the actual INSERT");
    };
    assert_eq!(insert.key, bytes("chosen"));
    assert_eq!(insert.expires_at, EXPIRES_AT);

    // Later in-place list writes must not alter the queued snapshot result.
    let items = ["RPUSH", "chosen", "later"].map(|value| Value::BulkString(Some(bytes(value))));
    apply(
        &cache,
        RaftCommandFactory::init_lua()
            .parse_request(&items)
            .unwrap(),
    );
    let ValueObject::List(frozen) = &insert.value else {
        panic!("snapshot INSERT must contain a list");
    };
    assert_eq!(*frozen.lock(), VecDeque::from([bytes("b")]));

    let restored = MyCache::new(2).unwrap();
    seed(&restored, "chosen", list(&["a", "b"]));
    // A mixed-time full scan can observe a newly populated earlier key.
    seed(&restored, "earlier", list(&["must not pop"]));
    replay(&restored, &queue);
    replay(&restored, &queue);
    let db = &restored.databases[DB as usize].mocha;
    let chosen = db.get_entry(&bytes("chosen")).unwrap();
    assert_eq!(elements(&chosen.value), VecDeque::from([bytes("b")]));
    assert_eq!(chosen.expire_at, Some(EXPIRES_AT));
    assert_eq!(chosen.value.version, queue[0].version);
    assert_eq!(
        elements(&db.get_entry(&bytes("earlier")).unwrap().value),
        VecDeque::from([bytes("must not pop")])
    );
    assert_eq!(restored.databases[0].mocha.len(), 0);
}

#[test]
fn snapshot_final_pop_records_delete_and_empty_or_wrong_type_records_nothing() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "chosen", list(&["one"]));
    let (_, queue) = snapshot(&cache, request(&["earlier", "chosen"], PopSide::Left));
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].expected_revision, OptionalU64::some(7));
    assert!(matches!(&queue[0].request, BaseOperation::Del(del) if del.key == bytes("chosen")));
    let restored = MyCache::new(2).unwrap();
    seed(&restored, "chosen", list(&["one"]));
    seed(&restored, "earlier", list(&["keep"]));
    replay(&restored, &queue);
    replay(&restored, &queue);
    assert!(
        restored.databases[DB as usize]
            .mocha
            .get_entry(&bytes("chosen"))
            .is_none()
    );
    assert_eq!(
        elements(
            &restored.databases[DB as usize]
                .mocha
                .get_entry(&bytes("earlier"))
                .unwrap()
                .value
        ),
        VecDeque::from([bytes("keep")])
    );

    let (result, queue) = snapshot(&cache, request(&["missing"], PopSide::Left));
    assert_eq!(result.encode(), b"*-1\r\n");
    assert!(queue.is_empty());
    seed(&cache, "wrong", wrong_type());
    let (result, queue) = snapshot(&cache, request(&["wrong"], PopSide::Left));
    assert!(matches!(result, Value::Error(_)));
    assert!(queue.is_empty());
}

#[test]
fn replicated_expiration_skips_expired_keys_and_exec_never_registers_waiters() {
    let cache = MyCache::new(2).unwrap();
    let mut expired = list(&["expired"]);
    expired.expire_at = Some(CLOCK);
    seed(&cache, "expired", expired);
    seed(&cache, "ready", list(&["available"]));
    let pop =
        |keys: &[&str]| Operation::Redis(RedisOperation::RedisBlPop(request(keys, PopSide::Left)));
    let result = apply(
        &cache,
        Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: vec![
                QueuedOperation::new(DB, pop(&["missing"])),
                QueuedOperation::new(DB, pop(&["expired", "ready"])),
                QueuedOperation::new(DB, pop(&["ready"])),
            ],
        })),
    );
    let Value::Array(Some(responses)) = result else {
        panic!("expected EXEC replies");
    };
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0].encode(), b"*-1\r\n");
    assert_eq!(responses[1].encode(), reply("ready", "available"));
    assert_eq!(responses[2].encode(), b"*-1\r\n");
    assert!(cache.list_blocking_keys.is_empty());
}

#[test]
fn parses_redis_second_timeouts_and_keeps_them_out_of_raft() {
    use super::{BlPopCommand, BlPopParams};
    use crate::protocol::raft_command::RaftCommand;
    use std::time::Duration;
    let args = |timeout: &str| {
        ["BLPOP", "key", "key", timeout]
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))))
    };
    for (text, milliseconds) in [
        ("0", None),
        ("-0", None),
        ("0.5", Some(500)),
        ("1e-4", Some(1)),
        ("0x1p-1", Some(500)),
        ("+01.25", Some(1250)),
    ] {
        let parsed = BlPopParams::parse(&args(text)).unwrap();
        assert_eq!(
            parsed.timeout,
            milliseconds.map(Duration::from_millis),
            "{text}"
        );
        assert_eq!(parsed.request.keys, vec![Bytes::from_static(b"key"); 2]);
    }
    let infinite = BlPopCommand.raft_request(&args("0")).unwrap();
    let finite = BlPopCommand.raft_request(&args("2.5")).unwrap();
    assert_eq!(
        bincode2::serialize(&infinite).unwrap(),
        bincode2::serialize(&finite).unwrap()
    );
    for (text, expected) in [
        ("-1", "ERR timeout is negative"),
        ("-0.0001", "ERR timeout is negative"),
        ("NaN", "ERR timeout is not a float or out of range"),
        (" 1", "ERR timeout is not a float or out of range"),
        ("1 ", "ERR timeout is not a float or out of range"),
        ("1e", "ERR timeout is not a float or out of range"),
        ("inf", "ERR timeout is out of range"),
        ("1e30", "ERR timeout is out of range"),
        ("9223372036854776", "ERR timeout is out of range"),
    ] {
        assert_eq!(
            BlPopParams::parse(&args(text)).unwrap_err().to_string(),
            expected,
            "{text}"
        );
    }
    assert_eq!(
        BlPopParams::parse(&args("0")[..2]).unwrap_err(),
        ProtocolError::WrongArgCount("blpop")
    );
}
