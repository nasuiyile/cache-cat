//! GEO commands exercise the same parsing, Raft encoding, and snapshot replay
//! boundaries as the sorted set that stores their index.

use crate::error::ProtocolError;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation, Request};
use bytes::Bytes;
use std::io::Cursor;

const PALERMO_HASH: &str = "sqc8b49rny0";
const CATANIA_HASH: &str = "sqdtr74hyu0";
const WRONG_TYPE: &[u8] = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

fn parse(args: &[&str]) -> Result<Operation, ProtocolError> {
    let items = args
        .iter()
        .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        .collect::<Vec<_>>();
    RaftCommandFactory::init_lua().parse_request(&items)
}

fn command(args: &[&str]) -> Operation {
    parse(args).unwrap_or_else(|error| panic!("{args:?}: {error}"))
}

fn apply(
    cache: &MyCache,
    operation: Operation,
    clock: u64,
    snapshot: Option<(&mut Vec<AtomicRequest>, &mut u64)>,
) -> Value {
    // Scores and options must survive the actual Raft request encoding.
    let request = Request::new(clock, 0, operation);
    let request: Request = bincode2::deserialize(&bincode2::serialize(&request).unwrap()).unwrap();
    let (clock, db_number) = request.split_u64();
    let mut update_type = match snapshot {
        Some((queue, revision)) => UpdateType::Snapshot { queue, revision },
        None => UpdateType::None,
    };
    let mut update = Update {
        effects: None,
        waiters: None,
        db_number,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
    };
    do_request(cache, request.operation, &mut update, true)
}

/// Run one client command: reads take the lease-read path, writes the log.
fn run(cache: &MyCache, args: &[&str]) -> Value {
    match parse(args) {
        Ok(Operation::Read(read)) => read_request(cache, read, 0, 1_000),
        Ok(operation) => apply(cache, operation, 1_000, None),
        Err(error) => error.into(),
    }
}

fn exec(commands: &[&[&str]]) -> Operation {
    Operation::Redis(RedisOperation::RedisExec(ExecParams {
        operations: commands
            .iter()
            .map(|args| QueuedOperation::new(0, command(args)))
            .collect(),
    }))
}

fn eval(script: &str, keys: &[&str], args: &[&str]) -> Operation {
    let bytes = |values: &[&str]| {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect::<Vec<_>>()
    };
    Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
        Bytes::copy_from_slice(script.as_bytes()),
        keys.len(),
        bytes(keys),
        bytes(args),
    )))
}

/// TTL and the members in score order, with scores as their 52-bit integers.
fn index(cache: &MyCache, key: &str) -> Option<(Option<u64>, Vec<(Bytes, u64)>)> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes())?;
    let ValueObject::ZSet(zset) = entry.value.data else {
        panic!("{key} is not a sorted set");
    };
    let members = zset
        .lock()
        .zrange(0, -1)
        .into_iter()
        .map(|(member, score)| (member, score as u64))
        .collect();
    Some((entry.expire_at, members))
}

#[test]
fn geoadd_builds_a_sorted_set_that_geohash_and_zset_commands_share() {
    let cache = MyCache::new(2).unwrap();
    let added = run(
        &cache,
        &[
            "GEOADD",
            "Sicily",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ],
    );
    assert_eq!(added.encode(), b":2\r\n");

    let hashes = run(
        &cache,
        &["GEOHASH", "Sicily", "Palermo", "missing", "Catania"],
    );
    assert_eq!(
        hashes.encode_proto(2),
        format!("*3\r\n$11\r\n{PALERMO_HASH}\r\n$-1\r\n$11\r\n{CATANIA_HASH}\r\n").as_bytes()
    );
    assert_eq!(
        hashes.encode_proto(3),
        format!("*3\r\n$11\r\n{PALERMO_HASH}\r\n_\r\n$11\r\n{CATANIA_HASH}\r\n").as_bytes()
    );

    // The index is an ordinary sorted set in both directions.
    assert_eq!(run(&cache, &["TYPE", "Sicily"]).encode(), b"+zset\r\n");
    assert_eq!(
        run(&cache, &["ZSCORE", "Sicily", "Palermo"]).encode_proto(2),
        b"$16\r\n3479099956230698\r\n"
    );
    assert_eq!(
        run(&cache, &["ZRANGE", "Sicily", "0", "-1"]).encode(),
        b"*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n"
    );
    assert_eq!(
        run(&cache, &["ZADD", "Sicily", "3479447370796909", "raw"]).encode(),
        b":1\r\n"
    );
    assert_eq!(
        run(&cache, &["GEOHASH", "Sicily", "raw"]).encode(),
        format!("*1\r\n$11\r\n{CATANIA_HASH}\r\n").as_bytes()
    );
    assert_eq!(
        run(&cache, &["ZREM", "Sicily", "Palermo", "Catania", "raw"]).encode(),
        b":3\r\n"
    );
    assert_eq!(index(&cache, "Sicily"), None);
    assert_eq!(
        run(&cache, &["GEOHASH", "Sicily", "Palermo"]).encode_proto(2),
        b"*1\r\n$-1\r\n"
    );

    assert_eq!(run(&cache, &["SET", "text", "value"]).encode(), b"+OK\r\n");
    for args in [
        vec!["GEOADD", "text", "1", "2", "member"],
        vec!["GEOADD", "text", "XX", "1", "2", "member"],
        vec!["GEOHASH", "text", "member"],
        vec!["GEOHASH", "text"],
    ] {
        assert_eq!(run(&cache, &args).encode(), WRONG_TYPE, "{args:?}");
    }
    // Arguments are validated before the key type is checked.
    assert_eq!(
        run(&cache, &["GEOADD", "text", "200", "0", "member"]).encode(),
        b"-ERR invalid longitude,latitude pair 200.000000,0.000000\r\n"
    );
}

#[test]
fn geoadd_options_count_and_update_members_like_zadd() {
    let cache = MyCache::new(2).unwrap();
    let integer = |args: &[&str]| match run(&cache, args) {
        Value::Integer(value) => value,
        other => panic!("{args:?}: {other:?}"),
    };
    let score = |member: &str| {
        index(&cache, "k")
            .unwrap()
            .1
            .into_iter()
            .find(|(name, _)| name == member)
            .map(|(_, score)| score)
    };

    // XX never creates the key.
    assert_eq!(integer(&["GEOADD", "k", "XX", "CH", "1", "1", "a"]), 0);
    assert_eq!(index(&cache, "k"), None);

    assert_eq!(integer(&["GEOADD", "k", "1", "1", "a", "2", "2", "b"]), 2);
    // An unchanged position is not a change, and moves only count with CH.
    assert_eq!(integer(&["GEOADD", "k", "CH", "1", "1", "a"]), 0);
    assert_eq!(integer(&["GEOADD", "k", "3", "3", "a"]), 0);
    assert_eq!(
        integer(&["GEOADD", "k", "CH", "4", "4", "a", "5", "5", "c"]),
        2
    );

    // NX adds new members and leaves existing ones in place.
    let before = score("a");
    assert_eq!(
        integer(&["GEOADD", "k", "NX", "CH", "6", "6", "a", "7", "7", "d"]),
        1
    );
    assert_eq!(score("a"), before);
    assert!(score("d").is_some());

    // XX moves existing members and adds none.
    assert_eq!(
        integer(&["GEOADD", "k", "XX", "CH", "8", "8", "a", "9", "9", "e"]),
        1
    );
    assert_ne!(score("a"), before);
    assert_eq!(score("e"), None);

    // A member repeated in one command is added once and then moved.
    assert_eq!(
        integer(&["GEOADD", "twice", "1", "1", "m", "2", "2", "m"]),
        1
    );
    assert_eq!(
        integer(&["GEOADD", "thrice", "CH", "1", "1", "m", "2", "2", "m"]),
        2
    );
    assert_eq!(index(&cache, "twice"), index(&cache, "thrice"));
    assert_eq!(index(&cache, "twice").unwrap().1.len(), 1);

    // Updating an index keeps its TTL.
    assert_eq!(integer(&["PEXPIRE", "k", "100000"]), 1);
    let ttl = index(&cache, "k").unwrap().0;
    assert!(ttl.is_some());
    assert_eq!(integer(&["GEOADD", "k", "10", "10", "f"]), 1);
    assert_eq!(index(&cache, "k").unwrap().0, ttl);
}

#[test]
fn geo_commands_run_inside_exec_and_lua() {
    let cache = MyCache::new(2).unwrap();
    let transaction = exec(&[
        &["GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"],
        &["GEOHASH", "Sicily", "Palermo", "missing"],
        &["GEOADD", "Sicily", "XX", "CH", "13.5", "38.2", "Palermo"],
        &[
            "GEOADD",
            "Sicily",
            "NX",
            "13.361389",
            "38.115556",
            "Palermo",
        ],
    ]);
    assert_eq!(
        apply(&cache, transaction, 10, None).encode_proto(2),
        format!("*4\r\n:1\r\n*2\r\n$11\r\n{PALERMO_HASH}\r\n$-1\r\n:1\r\n:0\r\n").as_bytes()
    );
    assert_eq!(run(&cache, &["DEL", "Sicily"]).encode(), b":1\r\n");

    // Lua passes the coordinates as numbers; a missing member becomes false.
    let script = eval(
        "redis.call('GEOADD', KEYS[1], 13.361389, 38.115556, 'Palermo') \
         local added = redis.call('GEOADD', KEYS[1], 'CH', 15.087269, 37.502669, ARGV[1]) \
         local hashes = redis.call('GEOHASH', KEYS[1], 'Palermo', ARGV[1], 'missing') \
         return {added, hashes[1], hashes[2], hashes[3] == false and 'absent' or 'present'}",
        &["Sicily"],
        &["Catania"],
    );
    assert_eq!(
        apply(&cache, script, 20, None).encode_proto(2),
        format!("*4\r\n:1\r\n$11\r\n{PALERMO_HASH}\r\n$11\r\n{CATANIA_HASH}\r\n$6\r\nabsent\r\n")
            .as_bytes()
    );

    // A rejected pair is an error reply for pcall and writes nothing.
    let rejected = eval(
        "return redis.pcall('GEOADD', KEYS[1], 1, 2, 'valid', 200, 0, 'invalid')",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        apply(&cache, rejected, 30, None).encode(),
        b"-ERR invalid longitude,latitude pair 200.000000,0.000000\r\n"
    );
    assert_eq!(index(&cache, "Sicily").unwrap().1.len(), 2);
}

async fn dump(cache: &MyCache) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    cache.dump_cache_to_writer(&mut bytes).await.unwrap();
    bytes.into_inner()
}

async fn restore(full: &[u8], queue: &[AtomicRequest], clock: u64) -> MyCache {
    let cache = MyCache::new(2).unwrap();
    cache.pause_expire_workers();
    cache
        .load_cache_from_reader(&mut Cursor::new(full))
        .await
        .unwrap();
    let queue: Vec<AtomicRequest> =
        bincode2::deserialize(&bincode2::serialize(queue).unwrap()).unwrap();
    for delta in queue {
        let mut update_type = UpdateType::CAS {
            expected_revision: delta.expected_revision,
            revision: delta.version,
        };
        let mut update = Update {
            effects: None,
            waiters: None,
            db_number: delta.db_number,
            write_clock: cache.set_write_clock(delta.write_clock),
            update_type: &mut update_type,
        };
        base_request(&cache, delta.request, &mut update);
    }
    cache.set_write_clock(clock);
    cache.resume_expire_workers();
    cache
}

#[tokio::test]
async fn geoadd_snapshot_cas_replays_every_scan_cut_with_ttl_and_recreation() {
    let cache = MyCache::new(2).unwrap();
    apply(
        &cache,
        command(&["GEOADD", "kept", "13.361389", "38.115556", "Palermo"]),
        1,
        None,
    );
    apply(&cache, command(&["PEXPIRE", "kept", "100000"]), 2, None);

    let operations = [
        command(&["GEOADD", "kept", "CH", "15.087269", "37.502669", "Catania"]),
        command(&["GEOADD", "kept", "XX", "CH", "13.5", "38.2", "Palermo"]),
        command(&["GEOADD", "fresh", "NX", "2.349014", "48.864716", "Paris"]),
        exec(&[
            &["GEOADD", "fresh", "-0.127758", "51.507351", "London"],
            &[
                "GEOADD",
                "kept",
                "NX",
                "0",
                "0",
                "Palermo",
                "12.496366",
                "41.902782",
                "Rome",
            ],
        ]),
        eval(
            "redis.call('GEOADD', KEYS[1], 'XX', 2.3, 48.9, 'Paris') \
             return redis.call('GEOADD', KEYS[2], 180, 85.05112878, 'corner')",
            &["fresh", "kept"],
            &[],
        ),
        command(&["DEL", "fresh"]),
        command(&["GEOADD", "fresh", "-180", "-85.05112878", "origin"]),
        command(&["ZREM", "kept", "Rome"]),
    ];

    let mut cuts = vec![dump(&cache).await];
    let mut queue = Vec::new();
    let mut revision = 7;
    for (step, operation) in operations.into_iter().enumerate() {
        let reply = apply(
            &cache,
            operation,
            step as u64 + 3,
            Some((&mut queue, &mut revision)),
        );
        assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        cuts.push(dump(&cache).await);
    }

    // Each GEOADD is recorded as itself, inside EXEC and Lua as well.
    let recorded = queue
        .iter()
        .filter(|delta| matches!(delta.request, BaseOperation::GeoAdd(_)))
        .count();
    assert_eq!(recorded, 8);
    assert_eq!(queue.len(), 10);

    // A GEOADD that changes nothing leaves no record and takes no revision.
    let before = (queue.len(), revision);
    let reply = apply(
        &cache,
        command(&["GEOADD", "absent", "XX", "1", "1", "member"]),
        20,
        Some((&mut queue, &mut revision)),
    );
    assert_eq!(reply.encode(), b":0\r\n");
    assert_eq!((queue.len(), revision), before);

    let expected = ["kept", "fresh", "absent"].map(|key| index(&cache, key));
    let (ttl, kept) = expected[0].clone().unwrap();
    assert!(ttl.is_some());
    // XX moved Palermo, NX then left it there, and ZREM removed Rome.
    assert_eq!(kept.len(), 3);
    assert_eq!(kept[0].0.as_ref(), b"Palermo");
    assert_ne!(kept[0].1, 3479099956230698);
    assert_eq!(
        kept[1..],
        [
            (Bytes::from_static(b"Catania"), 3479447370796909),
            (Bytes::from_static(b"corner"), 3 << 52),
        ]
    );
    assert_eq!(
        expected[1],
        Some((None, vec![(Bytes::from_static(b"origin"), 0)]))
    );
    assert_eq!(expected[2], None);
    for (cut, full) in cuts.iter().enumerate() {
        let restored = restore(full, &queue, cache.get_write_clock()).await;
        let actual = ["kept", "fresh", "absent"].map(|key| index(&restored, key));
        assert_eq!(actual, expected, "scan after operation {cut}");
    }
}
