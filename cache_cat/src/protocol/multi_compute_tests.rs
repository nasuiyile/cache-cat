use super::raft_command::RaftCommandFactory;
use crate::mocha::{ExpirePolicy, MochaOperation};
use crate::raft::types::core::mocha::cas::ComputedWrite;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::hll::RedisHll;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::{BaseOperation, InsertReq};
use crate::raft::types::entry::request::{AtomicRequest, Operation};
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;

const DB: u16 = 1;
const CLOCK: u64 = 1_000;
const EXPIRES_AT: u64 = 10_000;

fn request(args: &[&str]) -> Operation {
    let items = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua()
        .parse_request(&items)
        .unwrap()
}

#[test]
fn all_multi_key_writes_parse_as_redis_operations() {
    for args in [
        vec!["SUNIONSTORE", "dest", "source"],
        vec!["SINTERSTORE", "dest", "source"],
        vec!["SDIFFSTORE", "dest", "source"],
        vec!["PFMERGE", "dest", "source"],
        vec!["BITOP", "OR", "dest", "source"],
        vec!["RENAME", "source", "dest"],
        vec!["RENAMENX", "source", "dest"],
    ] {
        assert!(matches!(request(&args), Operation::Redis(_)), "{}", args[0]);
    }
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

fn snapshot(cache: &MyCache, args: &[&str]) -> (Value, Vec<AtomicRequest>) {
    let mut queue = Vec::new();
    let mut update_type = UpdateType::Snapshot(&mut queue);
    let mut update = Update {
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    let response = do_request(cache, request(args), &mut update, true);
    (response, queue)
}

fn replay(cache: &MyCache, queue: &[AtomicRequest]) {
    // Exercise the persisted representation, after any subsequent live writes.
    let bytes = bincode2::serialize(queue).unwrap();
    let queue: Vec<AtomicRequest> = bincode2::deserialize(&bytes).unwrap();
    for atomic in queue {
        assert_eq!(atomic.db_number, DB);
        assert_eq!(atomic.write_clock, CLOCK);
        assert!(matches!(
            &atomic.request,
            BaseOperation::Insert(_) | BaseOperation::Del(_)
        ));
        let mut update_type = UpdateType::CAS(atomic.version);
        let mut update = Update {
            db_number: atomic.db_number,
            write_clock: cache.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
        };
        base_request(cache, atomic.request, &mut update);
    }
    assert_eq!(cache.databases[0].mocha.len(), 0);
}

fn seed(cache: &MyCache, key: &str, value: ValueObject, expires_at: u64, version: u32) {
    for _ in 0..version {
        let response = apply(
            cache,
            Operation::Base(BaseOperation::Insert(InsertReq {
                key: Bytes::copy_from_slice(key.as_bytes()),
                value: value.clone(),
                expires_at,
            })),
        );
        assert_eq!(response.encode(), b"+OK\r\n");
    }
}

fn string(value: &str) -> ValueObject {
    ValueObject::String(Bytes::copy_from_slice(value.as_bytes()))
}

fn bytes_at(cache: &MyCache, key: &str) -> Bytes {
    let entry = cache.databases[DB as usize]
        .mocha
        .get_entry(key.as_bytes())
        .unwrap();
    let ValueObject::String(value) = entry.value.data else {
        panic!("{key} must contain a string");
    };
    value
}

fn members(value: &ValueObject) -> HashSet<Bytes> {
    let ValueObject::Set(set) = value else {
        panic!("expected a set");
    };
    set.lock().clone()
}

fn expected_members(values: &[&str]) -> HashSet<Bytes> {
    values
        .iter()
        .map(|value| Bytes::copy_from_slice(value.as_bytes()))
        .collect()
}

fn insert_record<'a>(
    queue: &'a [AtomicRequest],
    index: usize,
    key: &str,
    version: u32,
) -> &'a InsertReq {
    assert_eq!(queue[index].version, version);
    let BaseOperation::Insert(insert) = &queue[index].request else {
        panic!("expected a concrete INSERT");
    };
    assert_eq!(insert.key.as_ref(), key.as_bytes());
    insert
}

#[test]
fn all_set_stores_record_frozen_results_and_replay_without_sources() {
    for (command, expected) in [
        ("SUNIONSTORE", vec!["a", "b", "c"]),
        ("SINTERSTORE", vec!["b"]),
        ("SDIFFSTORE", vec!["a"]),
    ] {
        let cache = MyCache::new(2).unwrap();
        apply(&cache, request(&["SADD", "left", "a", "b"]));
        apply(&cache, request(&["SADD", "right", "b", "c"]));
        seed(&cache, "dest", string("old destination"), EXPIRES_AT, 3);
        let (response, queue) = snapshot(&cache, &[command, "dest", "left", "right"]);
        assert_eq!(
            response.encode(),
            format!(":{}\r\n", expected.len()).as_bytes()
        );
        assert_eq!(queue.len(), 1, "{command}");
        let expected = expected_members(&expected);
        assert_eq!(
            members(&insert_record(&queue, 0, "dest", 4).value),
            expected
        );
        assert_eq!(insert_record(&queue, 0, "dest", 4).expires_at, 0);

        // Sets use shared mutable storage: this must not alter the saved INSERT.
        apply(
            &cache,
            request(&["SADD", "dest", "later-destination-member"]),
        );
        apply(&cache, request(&["SADD", "left", "later-source-member"]));
        seed(&cache, "right", string("now the wrong type"), 0, 1);
        assert_eq!(
            members(&insert_record(&queue, 0, "dest", 4).value),
            expected,
            "{command} payload changed after SADD"
        );

        let restored = MyCache::new(2).unwrap();
        seed(&restored, "dest", string("old destination"), EXPIRES_AT, 3);
        seed(
            &restored,
            "left",
            string("source observed later by snapshot"),
            0,
            1,
        );
        // The other source is absent from this mixed-time full snapshot.
        replay(&restored, &queue);
        replay(&restored, &queue);
        let dest = restored.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(members(&dest.value.data), expected, "{command}");
        assert_eq!(dest.value.version, 4);
        assert_eq!(dest.expire_at, None);
    }
}

#[test]
fn empty_set_and_bitmap_results_record_versioned_deletes() {
    for args in [
        vec!["SUNIONSTORE", "dest", "missing"],
        vec!["SINTERSTORE", "dest", "missing"],
        vec!["SDIFFSTORE", "dest", "missing"],
        vec!["BITOP", "OR", "dest", "missing"],
    ] {
        let cache = MyCache::new(2).unwrap();
        seed(&cache, "dest", string("old"), EXPIRES_AT, 3);
        let (response, queue) = snapshot(&cache, &args);
        assert_eq!(response.encode(), b":0\r\n");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].version, 4);
        assert!(
            matches!(&queue[0].request, BaseOperation::Del(del) if del.key.as_ref() == b"dest")
        );
        let restored = MyCache::new(2).unwrap();
        seed(&restored, "dest", string("old"), EXPIRES_AT, 3);
        seed(&restored, "missing", string("source now exists"), 0, 1);
        replay(&restored, &queue);
        replay(&restored, &queue);
        assert!(
            restored.databases[DB as usize]
                .mocha
                .get_entry(b"dest".as_slice())
                .is_none()
        );
    }
}

#[test]
fn bitop_records_bytes_when_destination_is_also_a_source() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "dest", string("A"), EXPIRES_AT, 3);
    seed(&cache, "source", string("B"), 0, 1);
    let (response, queue) = snapshot(&cache, &["BITOP", "OR", "dest", "dest", "source"]);
    assert_eq!(response.encode(), b":1\r\n");
    assert_eq!(queue.len(), 1);
    assert_eq!(bytes_at(&cache, "dest").as_ref(), b"C");
    assert_eq!(insert_record(&queue, 0, "dest", 4).expires_at, 0);
    seed(&cache, "source", string("changed"), 0, 1);
    let restored = MyCache::new(2).unwrap();
    seed(&restored, "dest", string("A"), EXPIRES_AT, 3);
    replay(&restored, &queue);
    replay(&restored, &queue);
    assert_eq!(bytes_at(&restored, "dest").as_ref(), b"C");
    assert_eq!(
        restored.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap()
            .value
            .version,
        4
    );
}

fn hll(values: &[&str]) -> ValueObject {
    let mut hll = RedisHll::new();
    for value in values {
        hll.add(value.as_bytes());
    }
    ValueObject::String(hll.into_bytes())
}

#[test]
fn pfmerge_records_merged_hll_and_preserves_destination_expiration() {
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "dest", hll(&["one"]), EXPIRES_AT, 3);
    seed(&cache, "source", hll(&["two", "three"]), 0, 1);
    let (response, queue) = snapshot(&cache, &["PFMERGE", "dest", "source"]);
    assert_eq!(response.encode(), b"+OK\r\n");
    assert_eq!(queue.len(), 1);
    assert_eq!(insert_record(&queue, 0, "dest", 4).expires_at, EXPIRES_AT);
    let merged = bytes_at(&cache, "dest");
    assert_eq!(RedisHll::decode(&merged).unwrap().cardinality(), 3);
    seed(&cache, "source", string("invalid HLL"), 0, 1);
    let restored = MyCache::new(2).unwrap();
    seed(&restored, "dest", hll(&["one"]), EXPIRES_AT, 3);
    seed(&restored, "source", string("invalid HLL"), 0, 1);
    replay(&restored, &queue);
    replay(&restored, &queue);
    assert_eq!(bytes_at(&restored, "dest"), merged);
    let dest = restored.databases[DB as usize]
        .mocha
        .get_entry(b"dest".as_slice())
        .unwrap();
    assert_eq!(dest.value.version, 4);
    assert_eq!(dest.expire_at, Some(EXPIRES_AT));
}

#[test]
fn rename_variants_record_ordered_delete_and_insert_with_independent_versions() {
    for (command, destination_version, reply) in [
        ("RENAME", 3, b"+OK\r\n".as_slice()),
        ("RENAMENX", 0, b":1\r\n".as_slice()),
    ] {
        let cache = MyCache::new(2).unwrap();
        seed(&cache, "source", string("original source"), EXPIRES_AT, 2);
        seed(
            &cache,
            "dest",
            string("old destination"),
            0,
            destination_version,
        );
        let (response, queue) = snapshot(&cache, &[command, "source", "dest"]);
        assert_eq!(response.encode(), reply);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0].version, 3);
        assert!(
            matches!(&queue[0].request, BaseOperation::Del(del) if del.key.as_ref() == b"source")
        );
        let insert = insert_record(&queue, 1, "dest", destination_version + 1);
        assert_eq!(insert.expires_at, EXPIRES_AT);
        assert!(
            cache.databases[DB as usize]
                .mocha
                .get_entry(b"source".as_slice())
                .is_none()
        );
        assert_eq!(bytes_at(&cache, "dest").as_ref(), b"original source");

        let restored = MyCache::new(2).unwrap();
        // This source is from after the rename; its CAS delete must be skipped.
        seed(&restored, "source", string("newer source"), 0, 5);
        seed(
            &restored,
            "dest",
            string("old destination"),
            0,
            destination_version,
        );
        replay(&restored, &queue);
        replay(&restored, &queue);
        assert_eq!(bytes_at(&restored, "source").as_ref(), b"newer source");
        assert_eq!(bytes_at(&restored, "dest").as_ref(), b"original source");
        let dest = restored.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(dest.value.version, destination_version + 1);
        assert_eq!(dest.expire_at, Some(EXPIRES_AT));
    }
}

#[test]
fn rename_errors_and_noops_do_not_enqueue_or_modify_keys() {
    for command in ["RENAME", "RENAMENX"] {
        let cache = MyCache::new(2).unwrap();
        seed(&cache, "dest", string("original"), EXPIRES_AT, 3);
        for args in [
            [command, "missing", "dest"],
            [command, "missing", "missing"],
        ] {
            let (response, queue) = snapshot(&cache, &args);
            assert!(matches!(response, Value::Error(error) if error.contains("no such key")));
            assert!(queue.is_empty());
        }
        let (response, queue) = snapshot(&cache, &[command, "dest", "dest"]);
        let reply = if command == "RENAME" {
            b"+OK\r\n".as_slice()
        } else {
            b":0\r\n".as_slice()
        };
        assert_eq!(response.encode(), reply);
        assert!(
            queue.is_empty(),
            "same-key {command} must not modify its version"
        );
        let dest = cache.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(dest.value.version, 3);
        assert_eq!(dest.expire_at, Some(EXPIRES_AT));
        assert_eq!(bytes_at(&cache, "dest").as_ref(), b"original");
    }
    let cache = MyCache::new(2).unwrap();
    seed(&cache, "source", string("source"), EXPIRES_AT, 2);
    seed(&cache, "dest", string("dest"), 0, 3);
    let (response, queue) = snapshot(&cache, &["RENAMENX", "source", "dest"]);
    assert_eq!(response.encode(), b":0\r\n");
    assert!(queue.is_empty());
    assert_eq!(bytes_at(&cache, "source").as_ref(), b"source");
    assert_eq!(bytes_at(&cache, "dest").as_ref(), b"dest");
}

#[test]
fn invalid_source_types_do_not_write_or_enqueue_destinations() {
    for args in [
        vec!["SUNIONSTORE", "dest", "missing", "invalid"],
        vec!["SINTERSTORE", "dest", "missing", "invalid"],
        vec!["SDIFFSTORE", "dest", "missing", "invalid"],
        vec!["PFMERGE", "dest", "invalid"],
        vec!["BITOP", "OR", "dest", "missing", "invalid-set"],
    ] {
        let cache = MyCache::new(2).unwrap();
        seed(&cache, "dest", string("original"), EXPIRES_AT, 3);
        seed(&cache, "invalid", string("not a set or HLL"), 0, 1);
        apply(&cache, request(&["SADD", "invalid-set", "member"]));
        let (response, queue) = snapshot(&cache, &args);
        assert!(matches!(response, Value::Error(_)));
        assert!(queue.is_empty());
        assert_eq!(bytes_at(&cache, "dest").as_ref(), b"original");
        let dest = cache.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(dest.value.version, 3);
        assert_eq!(dest.expire_at, Some(EXPIRES_AT));
    }
}

#[test]
fn new_mutable_destinations_do_not_share_storage_with_snapshot_inserts() {
    for command in ["SUNIONSTORE", "RENAME", "RENAMENX"] {
        let cache = MyCache::new(2).unwrap();
        apply(&cache, request(&["SADD", "source", "original"]));
        let args = if command == "SUNIONSTORE" {
            vec![command, "dest", "source"]
        } else {
            vec![command, "source", "dest"]
        };
        let (_, queue) = snapshot(&cache, &args);
        let insert_index = queue.len() - 1;
        let expected = expected_members(&["original"]);
        assert_eq!(
            members(&insert_record(&queue, insert_index, "dest", 1).value),
            expected
        );
        apply(&cache, request(&["SADD", "dest", "later"]));
        assert_eq!(
            members(&insert_record(&queue, insert_index, "dest", 1).value),
            expected,
            "{command} shared its new destination with the snapshot queue"
        );
        let restored = MyCache::new(2).unwrap();
        replay(&restored, &queue);
        let dest = restored.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(members(&dest.value.data), expected);
    }
}

#[test]
fn normal_rename_moves_existing_container_without_cloning_its_contents() {
    for command in ["RENAME", "RENAMENX"] {
        let cache = MyCache::new(2).unwrap();
        apply(&cache, request(&["SADD", "source", "original"]));
        let source = cache.databases[DB as usize]
            .mocha
            .get_entry(b"source".as_slice())
            .unwrap();
        let ValueObject::Set(source_set) = source.value.data else {
            panic!("source must be a set");
        };
        apply(&cache, request(&[command, "source", "dest"]));
        let dest = cache.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        let ValueObject::Set(dest_set) = dest.value.data else {
            panic!("destination must be a set");
        };
        assert!(Arc::ptr_eq(&source_set, &dest_set), "{command}");
        assert!(
            cache.databases[DB as usize]
                .mocha
                .get_entry(b"source".as_slice())
                .is_none()
        );
    }
}

#[test]
fn computed_ttl_is_recorded_as_an_absolute_expiration() {
    let cache = MyCache::new(2).unwrap();
    let mut queue = Vec::new();
    let mut update_type = UpdateType::Snapshot(&mut queue);
    let mut update = Update {
        db_number: DB,
        write_clock: cache.set_write_clock(CLOCK),
        update_type: &mut update_type,
    };
    cache.execute_computed_writes(
        vec![ComputedWrite {
            key: Bytes::from_static(b"dest"),
            operation: MochaOperation::Insert {
                value: MyValue::new(string("expires")),
                expire: ExpirePolicy::Ttl(25),
            },
        }],
        &mut update,
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(insert_record(&queue, 0, "dest", 1).expires_at, CLOCK + 25);
    let restored = MyCache::new(2).unwrap();
    replay(&restored, &queue);
    for cache in [&cache, &restored] {
        let dest = cache.databases[DB as usize]
            .mocha
            .get_entry(b"dest".as_slice())
            .unwrap();
        assert_eq!(dest.expire_at, Some(CLOCK + 25));
        cache.set_write_clock(CLOCK + 25);
        assert!(
            cache.databases[DB as usize]
                .mocha
                .get_entry(b"dest".as_slice())
                .is_none()
        );
    }
}

#[test]
fn snapshot_replays_collection_removal_with_versioned_cas() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, request(&["SADD", "set", "member"]));
    // Advance the value version while retaining the same logical member.
    apply(&cache, request(&["SADD", "set", "member"]));
    apply(&cache, request(&["SADD", "set", "member"]));

    let (response, queue) = snapshot(&cache, &["SREM", "set", "member"]);
    assert_eq!(response.encode(), b":1\r\n");
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].version, 4);
    assert!(matches!(&queue[0].request, BaseOperation::SRem(_)));

    let restored = MyCache::new(2).unwrap();
    apply(&restored, request(&["SADD", "set", "member"]));
    apply(&restored, request(&["SADD", "set", "member"]));
    apply(&restored, request(&["SADD", "set", "member"]));
    for atomic in &queue {
        let mut update_type = UpdateType::CAS(atomic.version);
        let mut update = Update {
            db_number: atomic.db_number,
            write_clock: restored.set_write_clock(atomic.write_clock),
            update_type: &mut update_type,
        };
        base_request(&restored, atomic.request.clone(), &mut update);
    }
    assert!(
        restored.databases[DB as usize]
            .mocha
            .get_entry(b"set".as_slice())
            .is_none()
    );
}
