//! Command-boundary shrinking must preserve replicated order and live cursors.

use crate::protocol::hash::hdel::HDelCommand;
use crate::protocol::raft_command::{RaftCommand, RaftCommandFactory};
use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{do_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_map::ScanMap;
use crate::raft::types::core::structure::scan_set::ScanSet;
use crate::raft::types::core::structure::sorted_set::SortedSet;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::request::Operation;
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

fn member(index: usize) -> Bytes {
    format!("member:{index}").into()
}

fn cache() -> MyCache {
    let cache = MyCache::new(1).unwrap();
    cache.pause_expire_workers();
    cache.set_write_clock(100);
    cache
}

fn request(args: Vec<Bytes>) -> Operation {
    let args = args
        .into_iter()
        .map(|arg| Value::BulkString(Some(arg)))
        .collect::<Vec<_>>();
    // HDEL's normal command parser is available even though the existing Lua
    // factory does not register it. Exercise that ordinary entry point here.
    if args[0].string_bytes_clone().unwrap().as_ref() == b"HDEL" {
        HDelCommand.raft_request(&args).unwrap()
    } else {
        RaftCommandFactory::init_lua().parse_request(&args).unwrap()
    }
}

fn apply(cache: &MyCache, operation: Operation) -> Value {
    let mut update = Update {
        db_number: 0,
        write_clock: 100,
        update_type: &mut UpdateType::None,
        effects: None,
        waiters: None,
    };
    let reply = do_request(cache, operation, &mut update, true);
    assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
    reply
}

fn command(cache: &MyCache, args: &[&str]) -> Value {
    apply(
        cache,
        request(
            args.iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect(),
        ),
    )
}

fn remove(cache: &MyCache, verb: &str, key: &str, range: Range<usize>) {
    let count = range.len();
    let args = [
        Bytes::copy_from_slice(verb.as_bytes()),
        Bytes::copy_from_slice(key.as_bytes()),
    ]
    .into_iter()
    .chain(range.map(member))
    .collect();
    assert_eq!(
        apply(cache, request(args)).encode(),
        format!(":{count}\r\n").as_bytes()
    );
}

fn install_set_hash(cache: &MyCache, capacity: usize, count: usize, seed: u64) {
    let mut set = ScanSet::with_capacity_and_hasher(
        capacity,
        RandomState::with_seeds(seed, seed + 1, seed + 2, seed + 3),
    );
    let mut hash = ScanMap::with_capacity_and_hasher(
        capacity,
        RandomState::with_seeds(seed + 4, seed + 5, seed + 6, seed + 7),
    );
    // Same mutation history on each replica, deliberately unlike byte order.
    for offset in 0..count {
        let index = offset * 53 % count;
        set.insert(member(index));
        hash.insert(member(index), HashValue::Int(index as i64));
    }
    for (key, data) in [
        ("set", ValueObject::Set(Arc::new(Mutex::new(set)))),
        ("hash", ValueObject::Hash(Arc::new(Mutex::new(hash)))),
    ] {
        cache.databases[0]
            .mocha
            .insert_absolute(key.into(), MyValue::new(data), 100_000);
    }
}

fn allocated(cache: &MyCache, key: &str) -> usize {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
    match entry.value.data {
        ValueObject::Set(set) => set.lock().allocated_bytes(),
        ValueObject::Hash(hash) => hash.lock().allocated_bytes(),
        ValueObject::ZSet(zset) => zset.lock().index_allocated_bytes(),
        other => panic!("unexpected container: {other:?}"),
    }
}

fn state(cache: &MyCache, key: &str) -> Vec<(Bytes, Bytes)> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
    assert_eq!(entry.expire_at, Some(100_000));
    match entry.value.data {
        ValueObject::Set(set) => set
            .lock()
            .iter()
            .map(|member| (member.clone(), Bytes::new()))
            .collect(),
        ValueObject::Hash(hash) => hash
            .lock()
            .iter()
            .map(|(field, value)| (field.clone(), value.to_bytes()))
            .collect(),
        other => panic!("unexpected container: {other:?}"),
    }
}

#[test]
fn bulk_removals_shrink_only_sparse_replicas_and_preserve_subsequent_reads() {
    let oversized = cache();
    let compact = cache();
    install_set_hash(&oversized, 65_536, 1024, 1);
    install_set_hash(&compact, 2048, 1024, 101);
    for (verb, key) in [("SREM", "set"), ("HDEL", "hash")] {
        let before = allocated(&oversized, key);
        let compact_before = allocated(&compact, key);
        assert_eq!(
            command(&oversized, &[verb, key, "missing"]).encode(),
            b":0\r\n"
        );
        assert_eq!(
            allocated(&oversized, key),
            before,
            "no-op must not allocate"
        );
        for replica in [&oversized, &compact] {
            remove(replica, verb, key, 256..1024);
        }
        assert!(allocated(&oversized, key) < before / 4);
        assert_eq!(allocated(&compact, key), compact_before);
        assert_eq!(state(&oversized, key), state(&compact, key));
    }

    for args in [
        vec!["SRANDMEMBER", "set", "20"],
        vec!["SRANDMEMBER", "set", "-20"],
        vec!["HRANDFIELD", "hash", "20", "WITHVALUES"],
        vec!["SMEMBERS", "set"],
        vec!["HGETALL", "hash"],
        vec!["HVALS", "hash"],
        vec!["SPOP", "set", "9"],
        vec!["SADD", "set", "after-shrink", "after-shrink"],
        vec!["HSET", "hash", "after-shrink", "value"],
    ] {
        let left = command(&oversized, &args);
        let right = command(&compact, &args);
        for proto in [2, 3] {
            assert_eq!(left.encode_proto(proto), right.encode_proto(proto));
        }
        assert_eq!(state(&oversized, "set"), state(&compact, "set"));
        assert_eq!(state(&oversized, "hash"), state(&compact, "hash"));
    }
}

fn scan_page(cache: &MyCache, verb: &str, key: &str, cursor: u64) -> (u64, Vec<Bytes>) {
    let Operation::Read(read) = request(vec![
        Bytes::copy_from_slice(verb.as_bytes()),
        Bytes::copy_from_slice(key.as_bytes()),
        cursor.to_string().into(),
        "COUNT".into(),
        "1".into(),
    ]) else {
        panic!("expected scan read");
    };
    let Value::Array(Some(reply)) = read_request(cache, read, 0, 100) else {
        panic!("expected scan array");
    };
    let cursor = reply[0].string_bytes_clone().unwrap();
    let cursor = std::str::from_utf8(&cursor).unwrap().parse().unwrap();
    let Value::Array(Some(entries)) = &reply[1] else {
        panic!("expected scan entries");
    };
    let width = if verb == "SSCAN" { 1 } else { 2 };
    (
        cursor,
        entries
            .iter()
            .step_by(width)
            .map(|entry| entry.string_bytes_clone().unwrap())
            .collect(),
    )
}

#[test]
fn scan_cursors_cover_survivors_across_two_command_triggered_contractions() {
    for (scan, delete, key) in [
        ("SSCAN", "SREM", "set"),
        ("HSCAN", "HDEL", "hash"),
        ("ZSCAN", "ZREM", "zset"),
    ] {
        let cache = cache();
        if scan == "ZSCAN" {
            let mut zset = SortedSet::new();
            zset.zadd(ZAddReq {
                key: key.into(),
                nx: false,
                xx: false,
                gt: false,
                lt: false,
                ch: false,
                members: (0..32_768)
                    .map(|index| (member(index), index as f64))
                    .collect(),
            });
            cache.databases[0].mocha.insert_absolute(
                key.into(),
                MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(zset)))),
                100_000,
            );
        } else {
            install_set_hash(&cache, 32_768, 32_768, 7);
        }
        let (mut cursor, first) = scan_page(&cache, scan, key, 0);
        assert_ne!(cursor, 0);
        let mut seen: BTreeSet<_> = first.into_iter().collect();
        for range in [2048..32_768, 128..2048] {
            assert_ne!(cursor, 0, "{scan} ended before the next contraction");
            let before = allocated(&cache, key);
            remove(&cache, delete, key, range);
            assert!(
                allocated(&cache, key) < before,
                "{scan} did not reclaim storage"
            );
            let (next, page) = scan_page(&cache, scan, key, cursor);
            seen.extend(page);
            cursor = next;
        }
        for _ in 0..4096 {
            if cursor == 0 {
                break;
            }
            let (next, page) = scan_page(&cache, scan, key, cursor);
            seen.extend(page);
            cursor = next;
        }
        assert_eq!(cursor, 0, "{scan} did not terminate");
        let stable: BTreeSet<_> = (0..128).map(member).collect();
        assert!(
            stable.is_subset(&seen),
            "{scan} omitted a persistent member"
        );
        let remaining = command(&cache, &[delete, key, "missing"]);
        assert_eq!(remaining.encode(), b":0\r\n");
    }
}

#[test]
fn spop_and_set_store_results_reclaim_capacity_at_their_outer_boundary() {
    let oversized = cache();
    let compact = cache();
    install_set_hash(&oversized, 65_536, 4096, 5);
    install_set_hash(&compact, 4096, 4096, 17);
    let before = allocated(&oversized, "set");
    let left = command(&oversized, &["SPOP", "set", "3072"]);
    let right = command(&compact, &["SPOP", "set", "3072"]);
    assert_eq!(left.encode(), right.encode());
    let Value::Set(popped) = left else {
        panic!("expected count-form SPOP set");
    };
    assert_eq!(popped.len(), 3072);
    assert!(allocated(&oversized, "set") < before / 4);
    assert_eq!(state(&oversized, "set"), state(&compact, "set"));
    for cache in [&oversized, &compact] {
        assert_eq!(command(cache, &["SPOP", "set", "0"]).encode(), b"*0\r\n");
        command(cache, &["SPOP", "set", "10000"]);
        assert!(
            cache.databases[0]
                .mocha
                .get_entry(b"set".as_slice())
                .is_none()
        );
    }

    let cache = cache();
    install_set_hash(&cache, 8192, 8192, 31);
    let keep = [Bytes::from_static(b"SADD"), "keep".into()]
        .into_iter()
        .chain((0..64).map(member))
        .collect();
    apply(&cache, request(keep));
    let remove = [Bytes::from_static(b"SADD"), "remove".into()]
        .into_iter()
        .chain((64..8192).map(member))
        .collect();
    apply(&cache, request(remove));
    for (verb, source) in [("SINTERSTORE", "keep"), ("SDIFFSTORE", "remove")] {
        assert_eq!(
            command(&cache, &[verb, "result", "set", source]).encode(),
            b":64\r\n"
        );
        assert!(allocated(&cache, "result") < 32 * 1024);
        let entry = cache.databases[0]
            .mocha
            .get_entry(b"result".as_slice())
            .unwrap();
        assert_eq!(entry.expire_at, None, "store results must be persistent");
    }
}

#[test]
fn zpopmin_shrinks_its_member_index_and_preserves_remaining_ranks_and_scores() {
    let cache = cache();
    let mut zset = SortedSet::new();
    zset.zadd(ZAddReq {
        key: "zset".into(),
        nx: false,
        xx: false,
        gt: false,
        lt: false,
        ch: false,
        members: (0..8192)
            .map(|index| (member(index), index as f64))
            .collect(),
    });
    cache.databases[0].mocha.insert_absolute(
        "zset".into(),
        MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(zset)))),
        100_000,
    );
    let before = allocated(&cache, "zset");
    assert_eq!(
        command(&cache, &["ZPOPMIN", "zset", "0"]).encode(),
        b"*0\r\n"
    );
    assert_eq!(allocated(&cache, "zset"), before);
    let Value::MemberScores(popped) = command(&cache, &["ZPOPMIN", "zset", "7936"]) else {
        panic!("expected count-form ZPOPMIN scores");
    };
    assert_eq!(popped.len(), 7936);
    assert_eq!(popped.first(), Some(&(member(0), 0.0)));
    assert_eq!(popped.last(), Some(&(member(7935), 7935.0)));
    let after = allocated(&cache, "zset");
    assert!(
        before - after >= 64 * 1024,
        "ZPOPMIN did not reclaim its index"
    );
    assert_eq!(
        command(&cache, &["ZPOPMIN", "zset", "0"]).encode(),
        b"*0\r\n"
    );
    assert_eq!(allocated(&cache, "zset"), after);
    let entry = cache.databases[0]
        .mocha
        .get_entry(b"zset".as_slice())
        .unwrap();
    assert_eq!(entry.expire_at, Some(100_000));
    let ValueObject::ZSet(zset) = entry.value.data else {
        unreachable!()
    };
    let zset = zset.lock();
    assert_eq!(
        zset.zrange(0, -1),
        (7936..8192)
            .map(|index| (member(index), index as f64))
            .collect::<Vec<_>>()
    );
    for index in [7936, 8000, 8191] {
        assert_eq!(zset.zrank(&member(index)), Some((index - 7936) as i64));
        assert_eq!(zset.zscore(&member(index)), Some(index as f64));
    }
}
