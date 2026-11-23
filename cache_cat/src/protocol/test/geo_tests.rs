//! GEO commands exercise the same parsing, Raft encoding, and snapshot replay
//! boundaries as the sorted set that stores their index.

use crate::error::ProtocolError;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommandFactory;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request, read_request};
use crate::raft::types::core::response_value::{Value, format_double};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::{AtomicRequest, Operation, RedisOperation, Request};
use bytes::Bytes;
use std::io::Cursor;

const PALERMO_HASH: &str = "sqc8b49rny0";
const CATANIA_HASH: &str = "sqdtr74hyu0";
// Positions the Redis GEOPOS documentation prints for the two cities.
const PALERMO_POSITION: [&str; 2] = ["13.361389338970184", "38.1155563954963"];
const CATANIA_POSITION: [&str; 2] = ["15.087267458438873", "37.50266842333162"];
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

/// One GEOPOS position as RESP2 sends it.
fn position([longitude, latitude]: [&str; 2]) -> String {
    format!(
        "*2\r\n${}\r\n{longitude}\r\n${}\r\n{latitude}\r\n",
        longitude.len(),
        latitude.len()
    )
}

/// Longitude and latitude GEOPOS replies with for each member.
fn positions(cache: &MyCache, key: &str, members: &[&str]) -> Vec<Option<(f64, f64)>> {
    let mut args = vec!["GEOPOS", key];
    args.extend_from_slice(members);
    let Value::Array(Some(replies)) = run(cache, &args) else {
        panic!("{args:?} did not reply with an array");
    };
    replies
        .into_iter()
        .map(|reply| match reply {
            Value::Array(None) => None,
            Value::Array(Some(pair)) => match pair[..] {
                [Value::Double(longitude), Value::Double(latitude)] => Some((longitude, latitude)),
                _ => panic!("{pair:?}"),
            },
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn geopos_reads_positions_from_geo_indexes_and_plain_sorted_sets() {
    let cache = MyCache::new(2).unwrap();
    run(
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

    // The reply of the Redis documentation example.
    let reply = run(
        &cache,
        &["GEOPOS", "Sicily", "Palermo", "Catania", "NonExisting"],
    );
    assert_eq!(
        String::from_utf8(reply.encode_proto(2)).unwrap(),
        format!(
            "*3\r\n{}{}*-1\r\n",
            position(PALERMO_POSITION),
            position(CATANIA_POSITION)
        )
    );
    assert_eq!(
        String::from_utf8(reply.encode_proto(3)).unwrap(),
        "*3\r\n*2\r\n,13.361389338970184\r\n,38.1155563954963\r\n\
         *2\r\n,15.087267458438873\r\n,37.50266842333162\r\n_\r\n"
    );

    // A member written by ZADD has a position as well.
    assert_eq!(
        run(&cache, &["ZADD", "Sicily", "3479447370796909", "raw"]).encode(),
        b":1\r\n"
    );
    let found = positions(&cache, "Sicily", &["raw", "Catania"]);
    assert!(found[0].is_some());
    assert_eq!(found[0], found[1]);

    // Missing keys and members are null arrays, and reading creates nothing.
    assert_eq!(
        run(&cache, &["GEOPOS", "absent", "a", "b"]).encode_proto(2),
        b"*2\r\n*-1\r\n*-1\r\n"
    );
    assert_eq!(index(&cache, "absent"), None);
    assert_eq!(run(&cache, &["GEOPOS", "Sicily"]).encode(), b"*0\r\n");

    assert_eq!(run(&cache, &["SET", "text", "value"]).encode(), b"+OK\r\n");
    for args in [vec!["GEOPOS", "text", "member"], vec!["GEOPOS", "text"]] {
        assert_eq!(run(&cache, &args).encode(), WRONG_TYPE, "{args:?}");
    }
    assert_eq!(
        run(&cache, &["GEOPOS"]).encode(),
        b"-ERR wrong number of arguments for 'geopos' command\r\n"
    );
}

#[test]
fn geoadd_maps_a_geopos_reply_back_to_the_same_score() {
    let cache = MyCache::new(2).unwrap();
    let places = [
        ("London", "-0.127758", "51.507351"),
        ("Quito", "-78.467834", "-0.180653"),
        ("Sydney", "151.2099", "-33.865143"),
        ("near zero", "0.0001", "-0.0001"),
        ("north-east", "180", "85.05112878"),
        ("south-west", "-180", "-85.05112878"),
    ];
    for (member, longitude, latitude) in places {
        assert_eq!(
            run(&cache, &["GEOADD", "index", longitude, latitude, member]).encode(),
            b":1\r\n"
        );
    }

    let members = places.map(|(member, ..)| member);
    for ((member, longitude, latitude), found) in
        places.into_iter().zip(positions(&cache, "index", &members))
    {
        // A position is the centre of its cell, at most 360 / 2^26 degrees wide.
        let (found_longitude, found_latitude) = found.unwrap();
        assert!((found_longitude - longitude.parse::<f64>().unwrap()).abs() < 1e-5);
        assert!((found_latitude - latitude.parse::<f64>().unwrap()).abs() < 1e-5);
        // Sent back as the text of the reply, it lands in the same cell.
        let added = run(
            &cache,
            &[
                "GEOADD",
                "copy",
                &format_double(found_longitude),
                &format_double(found_latitude),
                member,
            ],
        );
        assert_eq!(added.encode(), b":1\r\n", "{member}");
    }
    assert_eq!(index(&cache, "copy"), index(&cache, "index"));
}

#[test]
fn geopos_runs_inside_exec_and_lua() {
    let cache = MyCache::new(2).unwrap();
    // Reads in a transaction see the writes queued before them.
    let transaction = exec(&[
        &["GEOADD", "Sicily", "13.361389", "38.115556", "Palermo"],
        &["GEOPOS", "Sicily", "Palermo", "missing"],
        &["ZREM", "Sicily", "Palermo"],
        &["GEOPOS", "Sicily", "Palermo"],
    ]);
    assert_eq!(
        String::from_utf8(apply(&cache, transaction, 10, None).encode_proto(2)).unwrap(),
        format!(
            "*4\r\n:1\r\n*2\r\n{}*-1\r\n:1\r\n*1\r\n*-1\r\n",
            position(PALERMO_POSITION)
        )
    );

    // A script sees the RESP2 reply by default: strings, and false for a null.
    let script = eval(
        "redis.call('GEOADD', KEYS[1], 13.361389, 38.115556, 'Palermo') \
         local positions = redis.call('GEOPOS', KEYS[1], 'Palermo', 'missing') \
         return {positions[1][1], positions[1][2], \
                 positions[2] == false and 'absent' or 'present'}",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 20, None).encode_proto(2)).unwrap(),
        "*3\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n$6\r\nabsent\r\n"
    );

    // After redis.setresp(3) the coordinates are doubles and a null is nil.
    let script = eval(
        "redis.setresp(3) \
         local positions = redis.call('GEOPOS', KEYS[1], 'Palermo', 'missing') \
         return {positions[1][1], positions[1][2], type(positions[1][1].double), \
                 positions[2] == nil and 'nil' or 'other'}",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 30, None).encode_proto(3)).unwrap(),
        "*4\r\n,13.361389338970184\r\n,38.1155563954963\r\n$6\r\nnumber\r\n$3\r\nnil\r\n"
    );
}

#[test]
fn geodist_measures_members_of_geo_indexes_and_plain_sorted_sets() {
    let cache = MyCache::new(2).unwrap();
    run(
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

    // The replies of the Redis documentation example, in both protocols.
    for (unit, expected) in [
        (None, "$11\r\n166274.1516\r\n"),
        (Some("m"), "$11\r\n166274.1516\r\n"),
        (Some("km"), "$8\r\n166.2742\r\n"),
        (Some("MI"), "$8\r\n103.3182\r\n"),
        (Some("Ft"), "$11\r\n545518.8700\r\n"),
    ] {
        let mut args = vec!["GEODIST", "Sicily", "Palermo", "Catania"];
        args.extend(unit);
        let reply = run(&cache, &args);
        for proto in [2, 3] {
            assert_eq!(
                String::from_utf8(reply.encode_proto(proto)).unwrap(),
                expected,
                "{args:?}"
            );
        }
    }
    assert_eq!(
        run(&cache, &["GEODIST", "Sicily", "Catania", "Palermo", "km"]).encode(),
        b"$8\r\n166.2742\r\n"
    );
    assert_eq!(
        run(&cache, &["GEODIST", "Sicily", "Palermo", "Palermo"]).encode(),
        b"$6\r\n0.0000\r\n"
    );

    // A member written by ZADD is measured from the cell of its score.
    assert_eq!(
        run(&cache, &["ZADD", "Sicily", "3479447370796909", "raw"]).encode(),
        b":1\r\n"
    );
    assert_eq!(
        run(&cache, &["GEODIST", "Sicily", "raw", "Catania"]).encode(),
        b"$6\r\n0.0000\r\n"
    );
    assert_eq!(
        run(&cache, &["GEODIST", "Sicily", "raw", "Palermo"]).encode(),
        b"$11\r\n166274.1516\r\n"
    );

    // A missing key or member is a null, and reading creates nothing.
    for args in [
        ["GEODIST", "Sicily", "Foo", "Bar"],
        ["GEODIST", "Sicily", "Palermo", "Bar"],
        ["GEODIST", "Sicily", "Foo", "Catania"],
        ["GEODIST", "absent", "Palermo", "Catania"],
    ] {
        let reply = run(&cache, &args);
        assert_eq!(reply.encode_proto(2), b"$-1\r\n", "{args:?}");
        assert_eq!(reply.encode_proto(3), b"_\r\n", "{args:?}");
    }
    assert_eq!(index(&cache, "absent"), None);

    // The arguments are checked before the key is looked at.
    assert_eq!(run(&cache, &["SET", "text", "value"]).encode(), b"+OK\r\n");
    assert_eq!(
        run(&cache, &["GEODIST", "text", "a", "b"]).encode(),
        WRONG_TYPE
    );
    for key in ["text", "absent", "Sicily"] {
        assert_eq!(
            run(&cache, &["GEODIST", key, "Palermo", "Catania", "yd"]).encode(),
            b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n",
            "{key}"
        );
        assert_eq!(
            run(&cache, &["GEODIST", key, "Palermo", "Catania", "km", "m"]).encode(),
            b"-ERR syntax error\r\n",
            "{key}"
        );
    }
    for args in [
        &["GEODIST"][..],
        &["GEODIST", "Sicily"],
        &["GEODIST", "Sicily", "Palermo"],
    ] {
        assert_eq!(
            run(&cache, args).encode(),
            b"-ERR wrong number of arguments for 'geodist' command\r\n",
            "{args:?}"
        );
    }
}

#[test]
fn geodist_runs_inside_exec_and_lua() {
    let cache = MyCache::new(2).unwrap();
    // Reads in a transaction see the writes queued before them.
    let transaction = exec(&[
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
        &["GEODIST", "Sicily", "Palermo", "Catania", "km"],
        &["ZREM", "Sicily", "Catania"],
        &["GEODIST", "Sicily", "Palermo", "Catania", "km"],
    ]);
    assert_eq!(
        String::from_utf8(apply(&cache, transaction, 10, None).encode_proto(2)).unwrap(),
        "*4\r\n:2\r\n$8\r\n166.2742\r\n:1\r\n$-1\r\n"
    );

    // A script gets the distance as a string and false for a null, and may
    // write what it read.
    let script = eval(
        "redis.call('GEOADD', KEYS[1], 15.087269, 37.502669, 'Catania') \
         local distance = redis.call('GEODIST', KEYS[1], 'Palermo', 'Catania', ARGV[1]) \
         redis.call('SET', KEYS[2], distance) \
         local missing = redis.call('GEODIST', KEYS[1], 'Palermo', 'missing') \
         return {distance, type(distance), missing == false and 'absent' or 'present'}",
        &["Sicily", "distance"],
        &["mi"],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 20, None).encode_proto(2)).unwrap(),
        "*3\r\n$8\r\n103.3182\r\n$6\r\nstring\r\n$6\r\nabsent\r\n"
    );
    assert_eq!(
        run(&cache, &["GET", "distance"]).encode(),
        b"$8\r\n103.3182\r\n"
    );

    // After redis.setresp(3) the distance is still a string and a null is nil.
    let script = eval(
        "redis.setresp(3) \
         local distance = redis.call('GEODIST', KEYS[1], 'Palermo', 'Catania') \
         local missing = redis.call('GEODIST', KEYS[1], 'Palermo', 'missing') \
         return {distance, type(distance), missing == nil and 'nil' or 'other'}",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 30, None).encode_proto(3)).unwrap(),
        "*3\r\n$11\r\n166274.1516\r\n$6\r\nstring\r\n$3\r\nnil\r\n"
    );

    // A bad unit is an error reply for pcall.
    let rejected = eval(
        "return redis.pcall('GEODIST', KEYS[1], 'Palermo', 'Catania', 'yd')",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        apply(&cache, rejected, 40, None).encode(),
        b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n"
    );
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

const SICILY: [&str; 8] = [
    "GEOADD",
    "Sicily",
    "13.361389",
    "38.115556",
    "Palermo",
    "15.087269",
    "37.502669",
    "Catania",
];

fn text(reply: Value) -> String {
    String::from_utf8(reply.encode()).unwrap()
}

/// TTL and the members in score order, with the exact bits of each score.
fn scores(cache: &MyCache, key: &str) -> Option<(Option<u64>, Vec<(Bytes, u64)>)> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes())?;
    let ValueObject::ZSet(zset) = entry.value.data else {
        panic!("{key} is not a sorted set");
    };
    let members = zset
        .lock()
        .zrange(0, -1)
        .into_iter()
        .map(|(member, score)| (member, score.to_bits()))
        .collect();
    Some((entry.expire_at, members))
}

#[test]
fn georadius_searches_geo_indexes_and_plain_sorted_sets() {
    let cache = MyCache::new(2).unwrap();
    run(&cache, &SICILY);

    // The replies of the Redis documentation example. Coordinates are
    // written as Redis 8 writes them.
    let palermo = position(PALERMO_POSITION);
    let catania = position(CATANIA_POSITION);
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHDIST"]
        )),
        "*2\r\n*2\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n*2\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n"
    );
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "Sicily", "15", "37", "200", "km", "WITHCOORD"]
        )),
        format!("*2\r\n*2\r\n$7\r\nPalermo\r\n{palermo}*2\r\n$7\r\nCatania\r\n{catania}")
    );
    assert_eq!(
        text(run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "200",
                "km",
                "WITHDIST",
                "WITHCOORD"
            ]
        )),
        format!(
            "*2\r\n*3\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n{palermo}\
             *3\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n{catania}"
        )
    );
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "Sicily", "15", "37", "200", "km", "COUNT", "1"]
        )),
        "*1\r\n$7\r\nCatania\r\n"
    );

    // A member written by ZADD is found by the cell of its score, and equal
    // scores come in member order.
    run(&cache, &["ZADD", "Sicily", "3479447370796909", "raw"]);
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "Sicily", "15", "37", "100", "km", "WITHHASH"]
        )),
        "*2\r\n*2\r\n$7\r\nCatania\r\n:3479447370796909\r\n*2\r\n$3\r\nraw\r\n:3479447370796909\r\n"
    );
    // Small scores are cells in the south-west corner of the grid.
    run(&cache, &["ZADD", "plain", "1", "one", "2", "two"]);
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "plain", "-180", "-85.05112878", "1", "km"]
        )),
        "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"
    );
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUS", "plain", "15", "37", "200", "km"]
        )),
        "*0\r\n"
    );

    // A missing key is an empty array, and reading creates nothing.
    let absent = run(&cache, &["GEORADIUS", "absent", "15", "37", "200", "km"]);
    assert_eq!(absent.encode_proto(2), b"*0\r\n");
    assert_eq!(absent.encode_proto(3), b"*0\r\n");
    assert_eq!(index(&cache, "absent"), None);

    // The type of the key is checked before any argument but the arity.
    run(&cache, &["SET", "text", "value"]);
    for args in [
        &["GEORADIUS", "text", "15", "37", "200", "km"][..],
        &["GEORADIUS", "text", "x", "y", "far", "parsecs", "BOGUS"],
        &[
            "GEORADIUS",
            "text",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "target",
        ],
    ] {
        assert_eq!(run(&cache, args).encode(), WRONG_TYPE, "{args:?}");
    }
    for key in ["absent", "Sicily"] {
        assert_eq!(
            run(&cache, &["GEORADIUS", key, "15", "37", "200", "parsecs"]).encode(),
            b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n",
            "{key}"
        );
        assert_eq!(
            run(&cache, &["GEORADIUS", key, "15", "37", "200", "km", "ANY"]).encode(),
            b"-ERR the ANY argument requires COUNT argument\r\n",
            "{key}"
        );
    }
    assert_eq!(index(&cache, "target"), None);
    for args in [
        &["GEORADIUS"][..],
        &["GEORADIUS", "text"],
        &["GEORADIUS", "text", "15", "37", "200"],
    ] {
        assert_eq!(
            run(&cache, args).encode(),
            b"-ERR wrong number of arguments for 'georadius' command\r\n",
            "{args:?}"
        );
    }
}

#[test]
fn georadius_store_replaces_the_destination_with_a_sorted_set() {
    let cache = MyCache::new(2).unwrap();
    run(&cache, &SICILY);
    let sicily = index(&cache, "Sicily");

    // STORE copies the geohash scores, so the result is a GEO index again.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "200",
                "km",
                "STORE",
                "near"
            ]
        )
        .encode(),
        b":2\r\n"
    );
    assert_eq!(index(&cache, "near"), sicily);
    assert_eq!(run(&cache, &["TYPE", "near"]).encode(), b"+zset\r\n");
    assert_eq!(
        run(&cache, &["GEODIST", "near", "Palermo", "Catania", "km"]).encode(),
        b"$8\r\n166.2742\r\n"
    );

    // STOREDIST stores the unrounded distance in the unit of the radius.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "200",
                "km",
                "STOREDIST",
                "dist"
            ]
        )
        .encode(),
        b":2\r\n"
    );
    assert_eq!(
        text(run(&cache, &["ZRANGE", "dist", "0", "-1"])),
        "*2\r\n$7\r\nCatania\r\n$7\r\nPalermo\r\n"
    );
    for (member, kilometers) in [("Catania", 56.4413), ("Palermo", 190.4424)] {
        let Value::Double(score) = run(&cache, &["ZSCORE", "dist", member]) else {
            panic!("{member} has no score");
        };
        assert!((score - kilometers).abs() < 5e-5, "{member}: {score}");
        assert_ne!(score, kilometers, "{member} was rounded");
    }

    // The destination is replaced whatever it held and loses its TTL. COUNT
    // and the order choose what is stored.
    run(&cache, &["SET", "near", "text"]);
    run(&cache, &["PEXPIRE", "near", "100000"]);
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "200",
                "km",
                "COUNT",
                "1",
                "DESC",
                "STORE",
                "near"
            ]
        )
        .encode(),
        b":1\r\n"
    );
    assert_eq!(
        index(&cache, "near"),
        Some((
            None,
            vec![(Bytes::from_static(b"Palermo"), 3479099956230698)]
        ))
    );

    // Nothing found, or no source at all, deletes the destination.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "-120",
                "40",
                "1",
                "km",
                "STORE",
                "near"
            ]
        )
        .encode(),
        b":0\r\n"
    );
    assert_eq!(index(&cache, "near"), None);
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "absent",
                "15",
                "37",
                "200",
                "km",
                "STOREDIST",
                "dist"
            ]
        )
        .encode(),
        b":0\r\n"
    );
    assert_eq!(index(&cache, "dist"), None);
    assert_eq!(index(&cache, "absent"), None);

    // A rejected command leaves the destination alone.
    run(&cache, &["SET", "text", "value"]);
    for args in [
        &[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "text",
            "WITHDIST",
        ][..],
        &[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "text",
            "COUNT",
            "0",
        ],
        &[
            "GEORADIUS",
            "text",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "Sicily",
        ],
    ] {
        assert!(matches!(run(&cache, args), Value::Error(_)), "{args:?}");
    }
    assert_eq!(run(&cache, &["GET", "text"]).encode(), b"$5\r\nvalue\r\n");
    assert_eq!(index(&cache, "Sicily"), sicily);

    // The source may be its own destination.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "100",
                "km",
                "STORE",
                "Sicily"
            ]
        )
        .encode(),
        b":1\r\n"
    );
    assert_eq!(
        index(&cache, "Sicily"),
        Some((
            None,
            vec![(Bytes::from_static(b"Catania"), 3479447370796909)]
        ))
    );
}

#[test]
fn georadius_runs_inside_exec_and_lua() {
    let cache = MyCache::new(2).unwrap();
    // Searches in a transaction see the writes queued before them, and an
    // argument error is the reply of its own command, as in Redis.
    let transaction = exec(&[
        &SICILY,
        &[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "WITHDIST",
            "ASC",
        ],
        &[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "100",
            "km",
            "STORE",
            "near",
        ],
        &["ZREM", "Sicily", "Catania"],
        &["GEORADIUS", "Sicily", "15", "37", "100", "km"],
        &[
            "GEORADIUS",
            "near",
            "15",
            "37",
            "100",
            "km",
            "COUNT",
            "1",
            "ANY",
        ],
        &["GEORADIUS", "Sicily", "15", "37", "100", "parsecs"],
    ]);
    assert_eq!(
        text(apply(&cache, transaction, 10, None)),
        "*7\r\n:2\r\n\
         *2\r\n*2\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n*2\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n\
         :1\r\n:1\r\n*0\r\n*1\r\n$7\r\nCatania\r\n\
         -ERR unsupported unit provided. please use M, KM, FT, MI\r\n"
    );

    // A script gets nested tables with strings, and may store a result.
    let script = eval(
        "redis.call('GEOADD', KEYS[1], 15.087269, 37.502669, 'Catania') \
         local found = redis.call('GEORADIUS', KEYS[1], 15, 37, 200, ARGV[1], \
                                  'WITHDIST', 'WITHCOORD', 'WITHHASH', 'DESC') \
         local stored = redis.call('GEORADIUS', KEYS[1], 15, 37, 200, ARGV[1], 'STOREDIST', KEYS[2]) \
         local farthest = redis.call('ZRANGE', KEYS[2], -1, -1)[1] \
         local first = found[1] \
         return {#found, #first, first[1], first[2], first[3], type(first[4][1]), first[4][2], \
                 stored, farthest}",
        &["Sicily", "distances"],
        &["km"],
    );
    assert_eq!(
        text(apply(&cache, script, 20, None)),
        "*9\r\n:2\r\n:4\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n:3479099956230698\r\n\
         $6\r\nstring\r\n$16\r\n38.1155563954963\r\n:2\r\n$7\r\nPalermo\r\n"
    );
    assert_eq!(run(&cache, &["ZCARD", "distances"]).encode(), b":2\r\n");

    // After redis.setresp(3) the coordinates are doubles.
    let script = eval(
        "redis.setresp(3) \
         local found = redis.call('GEORADIUS', KEYS[1], 15, 37, 100, 'km', 'WITHCOORD') \
         local nothing = redis.call('GEORADIUS', KEYS[1], -120, 40, 1, 'km') \
         return {found[1][1], found[1][2][1], #nothing}",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 30, None).encode_proto(3)).unwrap(),
        "*3\r\n$7\r\nCatania\r\n,15.087267458438873\r\n:0\r\n"
    );

    // An argument error is an error reply for pcall.
    let rejected = eval(
        "return redis.pcall('GEORADIUS', KEYS[1], 15, 37, -1, 'km')",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        apply(&cache, rejected, 40, None).encode(),
        b"-ERR radius cannot be negative\r\n"
    );
}

#[tokio::test]
async fn georadius_store_snapshot_cas_replays_every_scan_cut() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, command(&SICILY), 1, None);
    apply(&cache, command(&["SET", "old", "text"]), 2, None);
    apply(&cache, command(&["ZADD", "copy", "1", "stale"]), 3, None);
    apply(&cache, command(&["PEXPIRE", "copy", "100000"]), 4, None);

    let operations = [
        command(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STORE",
            "near",
        ]),
        command(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "200",
            "km",
            "STOREDIST",
            "dist",
        ]),
        command(&["GEOADD", "Sicily", "15.1", "37.5", "Acireale"]),
        exec(&[
            &[
                "GEORADIUS",
                "Sicily",
                "15",
                "37",
                "100",
                "km",
                "STORE",
                "near",
            ],
            &[
                "GEORADIUS",
                "near",
                "15",
                "37",
                "100",
                "km",
                "COUNT",
                "1",
                "STOREDIST",
                "dist",
            ],
        ]),
        eval(
            "return redis.call('GEORADIUS', KEYS[1], 15, 37, 300, 'km', 'STORE', KEYS[2])",
            &["Sicily", "copy"],
            &[],
        ),
        // Nothing found, and a missing source, delete their destinations.
        command(&[
            "GEORADIUS",
            "Sicily",
            "-120",
            "40",
            "1",
            "km",
            "STORE",
            "near",
        ]),
        command(&["GEORADIUS", "absent", "15", "37", "1", "km", "STORE", "old"]),
        command(&[
            "GEORADIUS",
            "Sicily",
            "15",
            "37",
            "60",
            "km",
            "STORE",
            "Sicily",
        ]),
    ];

    let mut cuts = vec![dump(&cache).await];
    let mut queue = Vec::new();
    let mut revision = 7;
    for (step, operation) in operations.into_iter().enumerate() {
        let reply = apply(
            &cache,
            operation,
            step as u64 + 5,
            Some((&mut queue, &mut revision)),
        );
        assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        cuts.push(dump(&cache).await);
    }

    // A search is recorded as the write it computed, never as a search that
    // a replay would run against a source that has changed since.
    assert!(queue.iter().all(|delta| matches!(
        delta.request,
        BaseOperation::Insert(_) | BaseOperation::Del(_) | BaseOperation::GeoAdd(_)
    )));
    assert_eq!(
        queue
            .iter()
            .filter(|delta| matches!(delta.request, BaseOperation::Insert(_)))
            .count(),
        6
    );

    let keys = ["Sicily", "near", "dist", "copy", "old", "absent"];
    let expected = keys.map(|key| scores(&cache, key));
    // The last search kept the two members within 60 km of the centre.
    let (ttl, sicily) = expected[0].clone().unwrap();
    assert_eq!(ttl, None);
    assert_eq!(
        sicily
            .iter()
            .map(|(member, _)| member.as_ref())
            .collect::<Vec<_>>(),
        [&b"Acireale"[..], b"Catania"]
    );
    assert_eq!(expected[1], None);
    // The nearest member with its distance, which is not a geohash: Acireale
    // lies a little closer to the centre than Catania's 56.4413 km.
    let (ttl, dist) = expected[2].clone().unwrap();
    assert_eq!(ttl, None);
    assert_eq!(dist.len(), 1);
    assert_eq!(dist[0].0.as_ref(), b"Acireale");
    let kilometers = f64::from_bits(dist[0].1);
    assert!((56.0..56.4413).contains(&kilometers), "{kilometers}");
    // The script replaced the stale sorted set and its TTL.
    let (ttl, copy) = expected[3].clone().unwrap();
    assert_eq!(ttl, None);
    assert_eq!(copy.len(), 3);
    assert_eq!(expected[4], None);
    assert_eq!(expected[5], None);

    for (cut, full) in cuts.iter().enumerate() {
        let restored = restore(full, &queue, cache.get_write_clock()).await;
        let actual = keys.map(|key| scores(&restored, key));
        assert_eq!(actual, expected, "scan after operation {cut}");
    }
}

const AGRIGENTO: [&str; 5] = ["GEOADD", "Sicily", "13.583333", "37.316667", "Agrigento"];

#[test]
fn georadiusbymember_searches_around_the_position_of_a_member() {
    let cache = MyCache::new(2).unwrap();
    run(&cache, &AGRIGENTO);
    run(&cache, &SICILY);

    // The reply of the Redis documentation example.
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km"]
        )),
        "*2\r\n$9\r\nAgrigento\r\n$7\r\nPalermo\r\n"
    );

    // The command is GEORADIUS around the position GEOPOS reports for the
    // member, byte for byte.
    let (longitude, latitude) = positions(&cache, "Sicily", &["Agrigento"])[0].unwrap();
    let (longitude, latitude) = (format_double(longitude), format_double(latitude));
    for (radius, unit) in [("100", "km"), ("200", "km"), ("60", "mi"), ("0", "m")] {
        for options in [
            &[][..],
            &["WITHDIST", "WITHCOORD", "WITHHASH"],
            &["COUNT", "2", "DESC", "WITHDIST"],
            &["COUNT", "1", "ANY"],
        ] {
            let mut by_member = vec!["GEORADIUSBYMEMBER", "Sicily", "Agrigento", radius, unit];
            by_member.extend(options);
            let mut by_position = vec!["GEORADIUS", "Sicily", &longitude, &latitude, radius, unit];
            by_position.extend(options);
            let reply = run(&cache, &by_member);
            assert!(matches!(&reply, Value::Array(Some(found)) if !found.is_empty()));
            for proto in [2, 3] {
                assert_eq!(
                    reply.encode_proto(proto),
                    run(&cache, &by_position).encode_proto(proto),
                    "{by_member:?}"
                );
            }
        }
    }
    // So the member is at distance zero, and the others at their GEODIST.
    let Value::BulkString(Some(distance)) =
        run(&cache, &["GEODIST", "Sicily", "Agrigento", "Palermo", "km"])
    else {
        panic!("no distance");
    };
    let distance = String::from_utf8(distance.to_vec()).unwrap();
    assert!(distance.starts_with("90.97"), "{distance}");
    assert_eq!(
        text(run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "km",
                "WITHDIST"
            ]
        )),
        format!(
            "*2\r\n*2\r\n$9\r\nAgrigento\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nPalermo\r\n$7\r\n{distance}\r\n"
        )
    );

    // A member written by ZADD is a centre like any other.
    run(&cache, &["ZADD", "Sicily", "3479447370796909", "raw"]);
    assert_eq!(
        text(run(
            &cache,
            &["GEORADIUSBYMEMBER", "Sicily", "raw", "1", "m"]
        )),
        "*2\r\n$7\r\nCatania\r\n$3\r\nraw\r\n"
    );

    // Without a key nothing is checked but the options, and reading creates
    // nothing.
    for args in [
        &["GEORADIUSBYMEMBER", "absent", "Agrigento", "100", "km"][..],
        &[
            "GEORADIUSBYMEMBER",
            "absent",
            "nobody",
            "far",
            "parsecs",
            "WITHDIST",
        ],
    ] {
        let reply = run(&cache, args);
        assert_eq!(reply.encode_proto(2), b"*0\r\n", "{args:?}");
        assert_eq!(reply.encode_proto(3), b"*0\r\n", "{args:?}");
    }
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "absent",
                "nobody",
                "far",
                "parsecs",
                "ANY"
            ]
        )
        .encode(),
        b"-ERR the ANY argument requires COUNT argument\r\n"
    );
    assert_eq!(index(&cache, "absent"), None);

    // With a key the member is checked first, then the radius and its unit.
    for (args, error) in [
        (
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "nobody",
                "far",
                "parsecs",
                "ANY",
            ][..],
            "ERR could not decode requested zset member",
        ),
        (
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "far",
                "parsecs",
                "ANY",
            ],
            "ERR need numeric radius",
        ),
        (
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "-5",
                "parsecs",
                "ANY",
            ],
            "ERR radius cannot be negative",
        ),
        (
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "parsecs",
                "ANY",
            ],
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ),
        (
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "km",
                "ANY",
            ],
            "ERR the ANY argument requires COUNT argument",
        ),
    ] {
        assert_eq!(text(run(&cache, args)), format!("-{error}\r\n"), "{args:?}");
    }

    // The type of the key comes before all of that, and the arity before it.
    run(&cache, &["SET", "text", "value"]);
    for args in [
        &["GEORADIUSBYMEMBER", "text", "Agrigento", "100", "km"][..],
        &[
            "GEORADIUSBYMEMBER",
            "text",
            "nobody",
            "far",
            "parsecs",
            "BOGUS",
        ],
        &[
            "GEORADIUSBYMEMBER",
            "text",
            "Agrigento",
            "100",
            "km",
            "STORE",
            "target",
        ],
    ] {
        assert_eq!(run(&cache, args).encode(), WRONG_TYPE, "{args:?}");
    }
    assert_eq!(index(&cache, "target"), None);
    for args in [
        &["GEORADIUSBYMEMBER"][..],
        &["GEORADIUSBYMEMBER", "text"],
        &["GEORADIUSBYMEMBER", "text", "Agrigento", "100"],
    ] {
        assert_eq!(
            run(&cache, args).encode(),
            b"-ERR wrong number of arguments for 'georadiusbymember' command\r\n",
            "{args:?}"
        );
    }
}

#[test]
fn georadiusbymember_store_replaces_the_destination_with_a_sorted_set() {
    let cache = MyCache::new(2).unwrap();
    run(&cache, &AGRIGENTO);
    run(&cache, &SICILY);
    let (_, sicily) = index(&cache, "Sicily").unwrap();
    let scores_of = |names: &[&str]| {
        sicily
            .iter()
            .filter(|(member, _)| names.iter().any(|name| name.as_bytes() == member.as_ref()))
            .cloned()
            .collect::<Vec<_>>()
    };

    // STORE copies the geohash scores of the members found.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "km",
                "STORE",
                "near"
            ]
        )
        .encode(),
        b":2\r\n"
    );
    assert_eq!(
        index(&cache, "near"),
        Some((None, scores_of(&["Agrigento", "Palermo"])))
    );

    // STOREDIST stores the unrounded distances: zero for the centre.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "km",
                "STOREDIST",
                "dist"
            ]
        )
        .encode(),
        b":2\r\n"
    );
    assert_eq!(
        text(run(&cache, &["ZRANGE", "dist", "0", "-1"])),
        "*2\r\n$9\r\nAgrigento\r\n$7\r\nPalermo\r\n"
    );
    assert!(matches!(
        run(&cache, &["ZSCORE", "dist", "Agrigento"]),
        Value::Double(distance) if distance == 0.0
    ));
    assert!(matches!(
        run(&cache, &["ZSCORE", "dist", "Palermo"]),
        Value::Double(distance) if (90.97..90.98).contains(&distance)
    ));

    // The destination is replaced whatever it held and loses its TTL.
    run(&cache, &["SET", "near", "text"]);
    run(&cache, &["PEXPIRE", "near", "100000"]);
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "200",
                "km",
                "COUNT",
                "1",
                "DESC",
                "STORE",
                "near"
            ]
        )
        .encode(),
        b":1\r\n"
    );
    assert_eq!(index(&cache, "near"), Some((None, scores_of(&["Catania"]))));

    // With a key, a missing member or a bad radius writes nothing.
    for args in [
        &[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "nobody",
            "100",
            "km",
            "STORE",
            "near",
        ][..],
        &[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "far",
            "km",
            "STORE",
            "near",
        ],
        &[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "1",
            "km",
            "STORE",
            "near",
            "WITHDIST",
        ],
    ] {
        assert!(matches!(run(&cache, args), Value::Error(_)), "{args:?}");
    }
    assert_eq!(index(&cache, "near"), Some((None, scores_of(&["Catania"]))));

    // Without a key the destination is deleted, whatever the member and the
    // radius say.
    run(&cache, &["SET", "victim", "text"]);
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "absent",
                "nobody",
                "far",
                "parsecs",
                "STORE",
                "victim"
            ]
        )
        .encode(),
        b":0\r\n"
    );
    assert_eq!(run(&cache, &["EXISTS", "victim"]).encode(), b":0\r\n");
    assert_eq!(index(&cache, "absent"), None);

    // The source may be its own destination.
    assert_eq!(
        run(
            &cache,
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Agrigento",
                "100",
                "km",
                "STORE",
                "Sicily"
            ]
        )
        .encode(),
        b":2\r\n"
    );
    assert_eq!(
        index(&cache, "Sicily"),
        Some((None, scores_of(&["Agrigento", "Palermo"])))
    );
}

#[test]
fn georadiusbymember_runs_inside_exec_and_lua() {
    let cache = MyCache::new(2).unwrap();
    // Searches in a transaction see the writes queued before them. A member
    // that is gone by then is the error of its own command.
    let transaction = exec(&[
        &AGRIGENTO,
        &SICILY,
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km"],
        &[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "100",
            "km",
            "STORE",
            "near",
        ],
        &["ZREM", "Sicily", "Agrigento"],
        &["GEORADIUSBYMEMBER", "Sicily", "Agrigento", "100", "km"],
        &[
            "GEORADIUSBYMEMBER",
            "near",
            "Agrigento",
            "100",
            "km",
            "COUNT",
            "1",
        ],
        &["GEORADIUSBYMEMBER", "absent", "nobody", "far", "parsecs"],
    ]);
    assert_eq!(
        text(apply(&cache, transaction, 10, None)),
        "*8\r\n:1\r\n:2\r\n*2\r\n$9\r\nAgrigento\r\n$7\r\nPalermo\r\n:2\r\n:1\r\n\
         -ERR could not decode requested zset member\r\n\
         *1\r\n$9\r\nAgrigento\r\n*0\r\n"
    );

    // A script gets nested tables with strings, and may store a result.
    let script = eval(
        "local found = redis.call('GEORADIUSBYMEMBER', KEYS[1], 'Palermo', 200, ARGV[1], \
                                  'WITHDIST', 'ASC') \
         local stored = redis.call('GEORADIUSBYMEMBER', KEYS[1], 'Palermo', 200, ARGV[1], \
                                   'STOREDIST', KEYS[2]) \
         local farthest = redis.call('ZRANGE', KEYS[2], -1, -1)[1] \
         return {#found, found[1][1], found[1][2], found[2][1], found[2][2], stored, farthest}",
        &["Sicily", "distances"],
        &["km"],
    );
    assert_eq!(
        text(apply(&cache, script, 20, None)),
        "*7\r\n:2\r\n$7\r\nPalermo\r\n$6\r\n0.0000\r\n$7\r\nCatania\r\n$8\r\n166.2742\r\n\
         :2\r\n$7\r\nCatania\r\n"
    );
    assert_eq!(run(&cache, &["ZCARD", "distances"]).encode(), b":2\r\n");

    // After redis.setresp(3) the coordinates are doubles.
    let script = eval(
        "redis.setresp(3) \
         local found = redis.call('GEORADIUSBYMEMBER', KEYS[1], 'Catania', 1, 'km', 'WITHCOORD') \
         local nothing = redis.call('GEORADIUSBYMEMBER', KEYS[2], 'nobody', 'far', 'parsecs') \
         return {found[1][1], found[1][2][1], #nothing}",
        &["Sicily", "absent"],
        &[],
    );
    assert_eq!(
        String::from_utf8(apply(&cache, script, 30, None).encode_proto(3)).unwrap(),
        "*3\r\n$7\r\nCatania\r\n,15.087267458438873\r\n:0\r\n"
    );

    // A missing member is an error reply for pcall.
    let rejected = eval(
        "return redis.pcall('GEORADIUSBYMEMBER', KEYS[1], 'nobody', 1, 'km')",
        &["Sicily"],
        &[],
    );
    assert_eq!(
        apply(&cache, rejected, 40, None).encode(),
        b"-ERR could not decode requested zset member\r\n"
    );
}

#[tokio::test]
async fn georadiusbymember_store_snapshot_cas_replays_every_scan_cut() {
    let cache = MyCache::new(2).unwrap();
    apply(&cache, command(&AGRIGENTO), 1, None);
    apply(&cache, command(&SICILY), 2, None);
    apply(&cache, command(&["SET", "old", "text"]), 3, None);
    apply(&cache, command(&["ZADD", "copy", "1", "stale"]), 4, None);
    apply(&cache, command(&["PEXPIRE", "copy", "100000"]), 5, None);

    let operations = [
        command(&[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "100",
            "km",
            "STORE",
            "near",
        ]),
        command(&[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "200",
            "km",
            "STOREDIST",
            "dist",
        ]),
        command(&["GEOADD", "Sicily", "13.6", "37.3", "Porto"]),
        exec(&[
            &[
                "GEORADIUSBYMEMBER",
                "Sicily",
                "Palermo",
                "100",
                "km",
                "STORE",
                "near",
            ],
            &[
                "GEORADIUSBYMEMBER",
                "near",
                "Palermo",
                "100",
                "km",
                "COUNT",
                "1",
                "DESC",
                "STOREDIST",
                "dist",
            ],
        ]),
        eval(
            "return redis.call('GEORADIUSBYMEMBER', KEYS[1], 'Catania', 300, 'km', 'STORE', KEYS[2])",
            &["Sicily", "copy"],
            &[],
        ),
        // Without a key the destination is deleted.
        command(&[
            "GEORADIUSBYMEMBER",
            "absent",
            "nobody",
            "far",
            "parsecs",
            "STORE",
            "old",
        ]),
        command(&[
            "GEORADIUSBYMEMBER",
            "Sicily",
            "Agrigento",
            "10",
            "km",
            "STORE",
            "Sicily",
        ]),
    ];

    let mut cuts = vec![dump(&cache).await];
    let mut queue = Vec::new();
    let mut revision = 7;
    for (step, operation) in operations.into_iter().enumerate() {
        let reply = apply(
            &cache,
            operation,
            step as u64 + 6,
            Some((&mut queue, &mut revision)),
        );
        assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        cuts.push(dump(&cache).await);
    }

    // Each search is recorded as the write it computed, so a replay looks
    // neither the member nor its neighbours up again.
    assert!(queue.iter().all(|delta| matches!(
        delta.request,
        BaseOperation::Insert(_) | BaseOperation::Del(_) | BaseOperation::GeoAdd(_)
    )));
    assert_eq!(
        queue
            .iter()
            .filter(|delta| matches!(delta.request, BaseOperation::Insert(_)))
            .count(),
        6
    );

    let keys = ["Sicily", "near", "dist", "copy", "old", "absent"];
    let expected = keys.map(|key| scores(&cache, key));
    let names = |state: &Option<(Option<u64>, Vec<(Bytes, u64)>)>| {
        let (ttl, members) = state.clone().unwrap();
        assert_eq!(ttl, None);
        members
            .into_iter()
            .map(|(member, _)| String::from_utf8(member.to_vec()).unwrap())
            .collect::<Vec<_>>()
    };
    // The last search kept Agrigento and the member added two kilometers
    // from it.
    let mut sicily = names(&expected[0]);
    sicily.sort();
    assert_eq!(sicily, ["Agrigento", "Porto"]);
    let mut near = names(&expected[1]);
    near.sort();
    assert_eq!(near, ["Agrigento", "Palermo", "Porto"]);
    // The farthest member from Palermo within 100 km, with its distance.
    let dist = names(&expected[2]);
    assert_eq!(dist.len(), 1);
    assert_ne!(dist[0], "Palermo");
    let kilometers = f64::from_bits(expected[2].as_ref().unwrap().1[0].1);
    assert!((90.0..100.0).contains(&kilometers), "{kilometers}");
    // The script replaced the stale sorted set and its TTL.
    assert_eq!(names(&expected[3]).len(), 4);
    assert_eq!(expected[4], None);
    assert_eq!(expected[5], None);

    for (cut, full) in cuts.iter().enumerate() {
        let restored = restore(full, &queue, cache.get_write_clock()).await;
        let actual = keys.map(|key| scores(&restored, key));
        assert_eq!(actual, expected, "scan after operation {cut}");
    }
}
