//! Replicated reads must remain deterministic after rebuilding hash tables.

use crate::protocol::hash::{
    hgetall::HGetAllParams, hkeys::HKeysParams, hrandfield::HRandFieldParams,
    hstrlen::HStrLenParams, hvals::HValsParams,
};
use crate::protocol::key::keys::KeysParams;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::set::{
    sdiff::SDiffParams, sinter::SInterParams, smembers::SMembersParams,
    srandmember::SRandMemberParams, sunion::SUnionParams,
};
use crate::protocol::string::get::GetParams;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::do_request;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_map::ScanMap as HashMap;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

const LABELS: [&str; 11] = [
    "random",
    "unique",
    "repeated",
    "members",
    "hash",
    "fields",
    "values",
    "intersection",
    "union",
    "difference",
    "keys",
];

const SCRIPT: &str = r#"
local invoke = redis[ARGV[2]]
local function save(label, result)
    if type(result) ~= 'table' then result = {result} end
    local parts = {}
    for i, value in ipairs(result) do
        parts[i] = tostring(string.len(value)) .. ':' .. value
    end
    local packed = table.concat(parts)
    redis.call('SET', ARGV[1] .. label, packed)
    return packed
end
return {
    save('random', invoke('SRANDMEMBER', KEYS[1])),
    save('unique', invoke('SRANDMEMBER', KEYS[1], 12)),
    save('repeated', invoke('SRANDMEMBER', KEYS[1], -48)),
    save('members', invoke('SMEMBERS', KEYS[1])),
    save('hash', invoke('HGETALL', KEYS[3])),
    save('fields', invoke('HKEYS', KEYS[3])),
    save('values', invoke('HVALS', KEYS[3])),
    save('intersection', invoke('SINTER', KEYS[1], KEYS[2])),
    save('union', invoke('SUNION', KEYS[1], KEYS[2])),
    save('difference', invoke('SDIFF', KEYS[1], KEYS[2])),
    save('keys', invoke('KEYS', 'input:*'))
}
"#;

fn members() -> Vec<Bytes> {
    (0..32)
        .map(|index| Bytes::from(vec![b'm', index, 0, 0xff]))
        .collect()
}

fn fields() -> Vec<(Bytes, HashValue)> {
    [b"".as_slice(), b"\0", b"a", b"a\0", b"\x80", b"\xff"]
        .into_iter()
        .enumerate()
        .map(|(index, field)| {
            (
                Bytes::copy_from_slice(field),
                HashValue::Str(Bytes::from(vec![0xff, 0, 10 - index as u8])),
            )
        })
        .collect()
}

fn replica(reverse: bool, restore_values: bool) -> MyCache {
    let cache = MyCache::new(1).unwrap();
    cache.set_write_clock(1_000);
    let capacity = if reverse { 512 } else { 0 };
    let mut first_members = members();
    let mut hash_fields = fields();
    if reverse {
        first_members.reverse();
        hash_fields.reverse();
    }
    let mut first = HashSet::with_capacity(capacity);
    first.extend(first_members);
    let mut second = HashSet::with_capacity(capacity);
    second.extend(members().into_iter().skip(8).take(16));
    second.insert(Bytes::from_static(b"extra\xff"));
    let mut hash = HashMap::with_capacity(capacity);
    hash.extend(hash_fields);
    let mut entries = vec![
        (
            Bytes::from_static(b"input:bag"),
            ValueObject::Set(Arc::new(Mutex::new(first))),
        ),
        (
            Bytes::from_static(b"input:other"),
            ValueObject::Set(Arc::new(Mutex::new(second))),
        ),
        (
            Bytes::from_static(b"input:hash"),
            ValueObject::Hash(Arc::new(Mutex::new(hash))),
        ),
        (
            Bytes::from_static(b"input:\0"),
            ValueObject::String("value".into()),
        ),
        (
            Bytes::from_static(b"input:\x80"),
            ValueObject::String("value".into()),
        ),
        (
            Bytes::from_static(b"input:\xff"),
            ValueObject::String("value".into()),
        ),
    ];
    if reverse {
        entries.reverse();
    }
    for (key, data) in entries {
        let mut value = MyValue::new(data);
        if restore_values {
            // Snapshot decoding rebuilds containers with new random hash seeds.
            value = bincode2::deserialize(&bincode2::serialize(&value).unwrap()).unwrap();
        }
        cache.databases[0].mocha.insert_absolute(key, value, 10_000);
    }
    cache.databases[0].mocha.insert_absolute(
        "input:expired".into(),
        MyValue::new(ValueObject::String("expired".into())),
        2_000,
    );
    cache
}

fn replicas() -> [MyCache; 3] {
    let caches = [
        replica(false, false),
        replica(true, false),
        replica(true, true),
    ];
    // Advance only one replica's local clock past every input's expiry. The
    // replicated request must still observe those inputs at its own clock.
    assert!(caches[1].get_and_update_read_clock() > 10_000);
    caches
}

fn apply_log(cache: &MyCache, log: &[u8]) -> Value {
    let request: Request = bincode2::deserialize(log).unwrap();
    let (clock, db_number) = request.split_u64();
    let mut update_type = UpdateType::None;
    let mut update = Update {
        effects: None,
        waiters: None,
        db_number,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
    };
    do_request(cache, request.operation, &mut update, true)
}

#[test]
fn replicated_hstrlen_reads_match_across_rebuilt_replicas() {
    let caches = replicas();
    let operation = Operation::Read(ReadOperation::HStrLen(HStrLenParams {
        key: "input:hash".into(),
        field: "a\0".into(),
    }));
    let log = bincode2::serialize(&Request::new(4_000, 0, operation)).unwrap();
    let replies: Vec<_> = caches.iter().map(|cache| apply_log(cache, &log)).collect();

    assert!(replies.iter().all(|reply| reply.encode() == b":3\r\n"));
    assert!(
        replies
            .iter()
            .skip(1)
            .all(|reply| reply.encode() == replies[0].encode())
    );

    let expired_log = bincode2::serialize(&Request::new(
        11_000,
        0,
        Operation::Read(ReadOperation::HStrLen(HStrLenParams {
            key: "input:hash".into(),
            field: "a\0".into(),
        })),
    ))
    .unwrap();
    let expired_replies: Vec<_> = caches
        .iter()
        .map(|cache| apply_log(cache, &expired_log))
        .collect();
    assert!(
        expired_replies
            .iter()
            .all(|reply| reply.encode() == b":0\r\n")
    );
}

#[test]
fn replicated_hrandfield_reads_match_across_rebuilt_replicas() {
    let reads = [
        HRandFieldParams {
            key: "input:hash".into(),
            count: None,
            with_values: false,
        },
        HRandFieldParams {
            key: "input:hash".into(),
            count: Some(2),
            with_values: true,
        },
        HRandFieldParams {
            key: "input:hash".into(),
            count: Some(-20),
            with_values: false,
        },
    ];

    for (index, params) in reads.into_iter().enumerate() {
        let log = bincode2::serialize(&Request::new(
            4_000 + index as u64,
            0,
            Operation::Read(ReadOperation::HRandField(params)),
        ))
        .unwrap();
        let replies: Vec<_> = replicas()
            .iter()
            .map(|cache| apply_log(cache, &log))
            .collect();
        assert!(
            replies
                .iter()
                .skip(1)
                .all(|reply| reply.encode() == replies[0].encode()),
            "replicas disagreed for HRANDFIELD variant {index}"
        );
        match index {
            0 => assert!(matches!(replies[0], Value::BulkString(Some(_)))),
            1 => assert!(matches!(replies[0], Value::Array(Some(ref values)) if values.len() == 4)),
            2 => {
                assert!(matches!(replies[0], Value::Array(Some(ref values)) if values.len() == 20))
            }
            _ => unreachable!(),
        }
    }
}

fn eval(prefix: &str, api: &str, proto: u8) -> Operation {
    let mut params = EvalParams::new(
        SCRIPT.into(),
        3,
        vec![
            "input:bag".into(),
            "input:other".into(),
            "input:hash".into(),
        ],
        vec![
            Bytes::copy_from_slice(prefix.as_bytes()),
            Bytes::copy_from_slice(api.as_bytes()),
        ],
    );
    params.proto = proto;
    Operation::Redis(RedisOperation::RedisEval(params))
}

fn unpack(mut packed: &[u8]) -> Vec<Bytes> {
    let mut values = Vec::new();
    while !packed.is_empty() {
        let separator = packed.iter().position(|byte| *byte == b':').unwrap();
        let len: usize = std::str::from_utf8(&packed[..separator])
            .unwrap()
            .parse()
            .unwrap();
        packed = &packed[separator + 1..];
        values.push(Bytes::copy_from_slice(&packed[..len]));
        packed = &packed[len..];
    }
    values
}

fn assert_saved_results(cache: &MyCache, prefix: &str, reply: &Value) {
    let Value::Array(Some(results)) = reply else {
        panic!("expected successful Lua result array, got {reply:?}");
    };
    assert_eq!(results.len(), LABELS.len());
    let all_members = members();
    for (index, (label, result)) in LABELS.iter().zip(results).enumerate() {
        let saved = cache.execute_read(
            GetParams {
                key: format!("{prefix}{label}").into(),
            },
            0,
            cache.get_write_clock(),
        );
        assert_eq!(saved.encode(), result.encode(), "stored {label}");
        let Value::BulkString(Some(packed)) = result else {
            panic!("expected packed binary string for {label}: {result:?}");
        };
        let values = unpack(packed);
        if index < 3 {
            assert_eq!(values.len(), [1, 12, 48][index]);
            assert!(values.iter().all(|value| all_members.contains(value)));
            if index == 1 {
                assert_eq!(values.iter().collect::<HashSet<_>>().len(), 12);
            }
        } else {
            let expected = match *label {
                "members" => all_members.clone(),
                "hash" => fields()
                    .into_iter()
                    .flat_map(|(key, value)| [key, value.to_bytes()])
                    .collect(),
                "fields" => fields().into_iter().map(|(key, _)| key).collect(),
                "values" => fields()
                    .into_iter()
                    .map(|(_, value)| value.to_bytes())
                    .collect(),
                "intersection" => all_members[8..24].to_vec(),
                "union" => std::iter::once(Bytes::from_static(b"extra\xff"))
                    .chain(all_members.clone())
                    .collect(),
                "difference" => all_members[..8]
                    .iter()
                    .chain(&all_members[24..])
                    .cloned()
                    .collect(),
                "keys" => vec![
                    Bytes::from_static(b"input:\0"),
                    "input:bag".into(),
                    "input:hash".into(),
                    "input:other".into(),
                    Bytes::from_static(b"input:\x80"),
                    Bytes::from_static(b"input:\xff"),
                ],
                _ => unreachable!(),
            };
            assert_eq!(values, expected, "content of {label}");
        }
    }
}

#[test]
fn lua_reads_write_identical_binary_values_from_replicated_logs() {
    let caches = replicas();
    let mut clock = 4_000;
    for proto in [2, 3] {
        for api in ["call", "pcall"] {
            let prefix = format!("saved:{proto}:{api}:");
            let log =
                bincode2::serialize(&Request::new(clock, 0, eval(&prefix, api, proto))).unwrap();
            let mut baseline = None;
            for cache in &caches {
                let reply = apply_log(cache, &log);
                assert_saved_results(cache, &prefix, &reply);
                let encoded = reply.encode_proto(proto);
                if let Some(expected) = &baseline {
                    assert_eq!(&encoded, expected);
                } else {
                    baseline = Some(encoded);
                }
            }
            clock += 1;
        }
    }
}

#[test]
fn exec_reads_and_nested_lua_replay_identically_on_rebuilt_replicas() {
    let caches = replicas();
    let reads = vec![
        ReadOperation::SRandMember(SRandMemberParams {
            key: "input:bag".into(),
            count: None,
        }),
        ReadOperation::SRandMember(SRandMemberParams {
            key: "input:bag".into(),
            count: Some(12),
        }),
        ReadOperation::SRandMember(SRandMemberParams {
            key: "input:bag".into(),
            count: Some(-48),
        }),
        ReadOperation::SMembers(SMembersParams {
            key: "input:bag".into(),
        }),
        ReadOperation::HGetAll(HGetAllParams {
            key: "input:hash".into(),
        }),
        ReadOperation::HKeys(HKeysParams {
            key: "input:hash".into(),
        }),
        ReadOperation::HVals(HValsParams {
            key: "input:hash".into(),
        }),
        ReadOperation::SInter(SInterParams {
            keys: vec!["input:bag".into(), "input:other".into()],
        }),
        ReadOperation::SUnion(SUnionParams {
            keys: vec!["input:bag".into(), "input:other".into()],
        }),
        ReadOperation::SDiff(SDiffParams {
            keys: vec!["input:bag".into(), "input:other".into()],
        }),
        ReadOperation::Keys(KeysParams {
            pattern: "input:*".into(),
        }),
    ];
    let mut operations: Vec<_> = reads
        .into_iter()
        .map(|read| QueuedOperation::new(0, Operation::Read(read)))
        .collect();
    operations.push(QueuedOperation::new(0, eval("exec:", "pcall", 2)));
    let log = bincode2::serialize(&Request::new(
        4_000,
        0,
        Operation::Redis(RedisOperation::RedisExec(ExecParams { operations })),
    ))
    .unwrap();
    let replies: Vec<_> = caches.iter().map(|cache| apply_log(cache, &log)).collect();
    for (cache, reply) in caches.iter().zip(&replies) {
        let Value::Array(Some(results)) = reply else {
            panic!("expected EXEC result array, got {reply:?}");
        };
        assert_eq!(results.len(), LABELS.len() + 1);
        assert!(matches!(&results[3], Value::Set(members) if members.len() == 32));
        assert!(matches!(&results[4], Value::Map(fields) if fields.len() == 6));
        assert_saved_results(cache, "exec:", results.last().unwrap());
        for proto in [2, 3] {
            assert_eq!(reply.encode_proto(proto), replies[0].encode_proto(proto));
        }
    }
}


#[test]
fn hscan_cursor_from_ordinary_read_is_deterministic_in_exec_and_lua_after_restore() {
    use crate::protocol::hash::hscan::HScanCommand;
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::mocha::request_handler::read_request;

    for proto in [2, 3] {
        let caches = replicas();
        let arguments: Vec<_> = ["HSCAN", "input:hash", "0", "COUNT", "1"]
            .into_iter()
            .map(|value| Value::BulkString(Some(value.into())))
            .collect();
        let Operation::Read(read) = HScanCommand.raft_request(&arguments).unwrap() else {
            panic!("HSCAN must be a read");
        };
        let Value::Array(Some(page)) = read_request(&caches[0], read, 0, 4_000) else {
            panic!("HSCAN must return an array");
        };
        let cursor = page[0].string_bytes_clone().unwrap();
        let mut continuation = arguments;
        continuation[2] = Value::BulkString(Some(cursor.clone()));
        let scan = HScanCommand.raft_request(&continuation).unwrap();
        let mut script = EvalParams::new(
            Bytes::from_static(br#"
                local result = redis.call('HSCAN', KEYS[1], ARGV[1], 'COUNT', 1)
                redis.call('SET', KEYS[2], result[2][1])
                redis.call('HSET', KEYS[1], result[2][1], 'written')
                return result
            "#),
            2,
            vec!["input:hash".into(), "scan:first".into()],
            vec![cursor],
        );
        script.proto = proto;
        let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: vec![
                QueuedOperation::new(0, scan.clone()),
                QueuedOperation::new(0, Operation::Redis(RedisOperation::RedisEval(script))),
                QueuedOperation::new(0, scan),
            ],
        }));
        let log = bincode2::serialize(&Request::new(4_000, 0, operation)).unwrap();
        let replies: Vec<_> = caches.iter().map(|cache| apply_log(cache, &log)).collect();
        for (cache, reply) in caches.iter().zip(&replies) {
            assert_eq!(reply.encode_proto(proto), replies[0].encode_proto(proto));
            let Value::Array(Some(results)) = reply else {
                panic!("expected successful EXEC, got {reply:?}");
            };
            assert_eq!(results.len(), 3);
            for result in results {
                let Value::Array(Some(page)) = result else {
                    panic!("expected scan response, got {result:?}");
                };
                assert_eq!(page[0].string_bytes_clone().unwrap().as_ref(), b"0");
                let Value::Array(Some(values)) = &page[1] else {
                    panic!("expected flat scan values");
                };
                assert_eq!(values.len(), fields().len() * 2);
                assert_eq!(values[0].string_bytes_clone().unwrap(), Bytes::new());
            }
            assert_eq!(
                cache
                    .execute_read(GetParams { key: "scan:first".into() }, 0, 4_000)
                    .encode(),
                Value::BulkString(Some(Bytes::new())).encode(),
            );
            let hash = cache.databases[0]
                .mocha
                .get(b"input:hash".as_slice())
                .unwrap();
            let ValueObject::Hash(hash) = hash.data else {
                panic!("expected hash");
            };
            assert_eq!(
                hash.lock().get(b"".as_slice()).unwrap().to_bytes().as_ref(),
                b"written"
            );
        }
    }
}
