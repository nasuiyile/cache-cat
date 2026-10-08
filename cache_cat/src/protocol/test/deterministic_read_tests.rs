//! Replicated reads must remain deterministic after rebuilding hash tables.

use crate::protocol::hash::{hgetall::HGetAllParams, hkeys::HKeysParams, hvals::HValsParams};
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
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
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
        db_number,
        write_clock: cache.set_write_clock(clock),
        update_type: &mut update_type,
    };
    do_request(cache, request.operation, &mut update, true)
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
