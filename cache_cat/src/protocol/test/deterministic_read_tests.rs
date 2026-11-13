//! Replicated reads must remain deterministic after rebuilding hash tables.

use crate::protocol::hash::{
    hdel::HDelReq, hgetall::HGetAllParams, hkeys::HKeysParams, hrandfield::HRandFieldParams,
    hset::HSetReq, hstrlen::HStrLenParams, hvals::HValsParams,
};
use crate::protocol::key::keys::KeysParams;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::set::{
    sdiff::SDiffParams, sinter::SInterParams, smembers::SMembersParams,
    srandmember::SRandMemberParams, sunion::SUnionParams,
};
use crate::protocol::string::get::GetParams;
use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::store::statemachine::{RaftMetaData, SnapshotState};
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::{base_request, do_request, read_request};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_map::ScanMap as HashMap;
use crate::raft::types::core::structure::scan_set::ScanSet;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use ahash::RandomState;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

const LABELS: [&str; 14] = [
    "random",
    "unique",
    "repeated",
    "members",
    "hash",
    "fields",
    "values",
    "hrandom",
    "hunique",
    "hrepeated",
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
    save('hrandom', invoke('HRANDFIELD', KEYS[3])),
    save('hunique', invoke('HRANDFIELD', KEYS[3], 3, 'WITHVALUES')),
    save('hrepeated', invoke('HRANDFIELD', KEYS[3], -16, 'WITHVALUES')),
    save('intersection', invoke('SINTER', KEYS[1], KEYS[2])),
    save('union', invoke('SUNION', KEYS[1], KEYS[2])),
    save('difference', invoke('SDIFF', KEYS[1], KEYS[2])),
    save('keys', invoke('KEYS', 'input:*'))
}
"#;

fn members() -> Vec<Bytes> {
    (0..32_usize)
        .map(|index| Bytes::from(vec![b'm', (index * 13 % 32) as u8, 0, 0xff]))
        .collect()
}

fn fields() -> Vec<(Bytes, HashValue)> {
    [b"a".as_slice(), b"\xff", b"\0", b"a\0", b"", b"\x80"]
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
    cache.pause_expire_workers();
    cache.set_write_clock(1_000);
    let capacity = if reverse { 512 } else { 0 };
    let first_members = members();
    let hash_fields = fields();
    let seed = if reverse { 101 } else { 1 };
    let hasher = RandomState::with_seeds(seed, seed + 1, seed + 2, seed + 3);
    let mut first = ScanSet::with_capacity_and_hasher(capacity, hasher.clone());
    first.extend(first_members.iter().cloned());
    first.insert(first_members[0].clone());
    first.remove(&first_members[3]);
    first.insert(first_members[3].clone());
    let mut second = ScanSet::with_capacity_and_hasher(capacity, hasher.clone());
    second.extend(members().into_iter().skip(8).take(16));
    second.insert(Bytes::from_static(b"extra\xff"));
    let mut hash = HashMap::with_capacity_and_hasher(capacity, hasher);
    hash.extend(hash_fields.iter().cloned());
    hash.insert(hash_fields[0].0.clone(), hash_fields[0].1.clone());
    hash.remove(&hash_fields[1].0);
    hash.insert(hash_fields[1].0.clone(), hash_fields[1].1.clone());
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
        // All replicas share the same non-lexicographical member/field history.
        // Only their local index layout, cloning and snapshot decoding differ.
        let data = if reverse { data.snapshot_clone() } else { data };
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

fn dense_members(cache: &MyCache, key: &str) -> Vec<Bytes> {
    let entry = cache.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
    let ValueObject::Set(set) = entry.value.data else {
        panic!("expected Set at {key}");
    };
    set.lock().iter().cloned().collect()
}

fn dense_fields(cache: &MyCache) -> Vec<(Bytes, Bytes)> {
    let entry = cache.databases[0]
        .mocha
        .get_entry(b"input:hash".as_slice())
        .unwrap();
    let ValueObject::Hash(hash) = entry.value.data else {
        panic!("expected Hash");
    };
    hash.lock()
        .iter()
        .map(|(key, value)| (key.clone(), value.to_bytes()))
        .collect()
}

fn input_state(cache: &MyCache) -> Vec<Vec<u8>> {
    ["input:bag", "input:other", "input:hash"]
        .into_iter()
        .map(|key| {
            // Serialization retains sequence, content, TTL and local revision,
            // while excluding each container's local hash seed and capacity.
            let entry = cache.databases[0].mocha.get_entry(key.as_bytes()).unwrap();
            bincode2::serialize(&entry).unwrap()
        })
        .collect()
}

fn flat_bytes(reply: &Value) -> Vec<Bytes> {
    match reply {
        Value::BulkString(Some(value)) => vec![value.clone()],
        Value::Array(Some(values)) | Value::Set(values) => values
            .iter()
            .map(|value| value.string_bytes_clone().unwrap())
            .collect(),
        Value::Map(fields) => fields
            .iter()
            .flat_map(|(field, value)| {
                [
                    field.string_bytes_clone().unwrap(),
                    value.string_bytes_clone().unwrap(),
                ]
            })
            .collect(),
        _ => panic!("expected strings in reply, got {reply:?}"),
    }
}

fn assert_sample(reply: &Value, population: &[Bytes], count: Option<i64>, stride: usize) {
    let values = flat_bytes(reply);
    let expected_len = match count {
        None => 1,
        Some(count) if count >= 0 => (count as usize).min(population.len()),
        Some(count) => count.unsigned_abs() as usize,
    };
    assert_eq!(values.len(), expected_len * stride);
    let selected = values.iter().step_by(stride).collect::<Vec<_>>();
    assert!(selected.iter().all(|value| population.contains(value)));
    if count.is_none_or(|count| count >= 0) {
        assert_eq!(selected.iter().collect::<HashSet<_>>().len(), expected_len);
    }
}

#[test]
fn dense_reads_preserve_unsorted_sequences_and_sampling_never_mutates_inputs() {
    let caches = replicas();
    let members = dense_members(&caches[0], "input:bag");
    let fields = dense_fields(&caches[0]);
    assert!(!members.is_sorted());
    assert!(!fields.iter().map(|(field, _)| field).is_sorted());
    let field_names = fields
        .iter()
        .map(|(field, _)| field.clone())
        .collect::<Vec<_>>();
    let reads = [
        (
            ReadOperation::SMembers(SMembersParams {
                key: "input:bag".into(),
            }),
            members.clone(),
        ),
        (
            ReadOperation::HGetAll(HGetAllParams {
                key: "input:hash".into(),
            }),
            fields
                .iter()
                .flat_map(|(field, value)| [field.clone(), value.clone()])
                .collect(),
        ),
        (
            ReadOperation::HVals(HValsParams {
                key: "input:hash".into(),
            }),
            fields.iter().map(|(_, value)| value.clone()).collect(),
        ),
    ];
    for cache in &caches {
        let before = input_state(cache);
        assert_eq!(before, input_state(&caches[0]));
        for (read, expected) in &reads {
            let ordinary = read_request(cache, read.clone(), 0, 4_000);
            assert_eq!(&flat_bytes(&ordinary), expected);
            let log = bincode2::serialize(&Request::new(4_000, 0, Operation::Read(read.clone())))
                .unwrap();
            let replicated = apply_log(cache, &log);
            assert_eq!(&flat_bytes(&replicated), expected);
        }
        for count in [None, Some(0), Some(3), Some(999), Some(-48)] {
            let read = ReadOperation::SRandMember(SRandMemberParams {
                key: "input:bag".into(),
                count,
            });
            assert_sample(
                &read_request(cache, read.clone(), 0, 4_000),
                &members,
                count,
                1,
            );
            let log = bincode2::serialize(&Request::new(4_000, 0, Operation::Read(read))).unwrap();
            assert_sample(&apply_log(cache, &log), &members, count, 1);
        }
        for (count, with_values) in [
            (None, false),
            (Some(0), true),
            (Some(3), false),
            (Some(3), true),
            (Some(999), true),
            (Some(-20), false),
            (Some(-20), true),
        ] {
            let read = ReadOperation::HRandField(HRandFieldParams {
                key: "input:hash".into(),
                count,
                with_values,
            });
            let log = bincode2::serialize(&Request::new(4_000, 0, Operation::Read(read.clone())))
                .unwrap();
            for reply in [read_request(cache, read, 0, 4_000), apply_log(cache, &log)] {
                assert_sample(&reply, &field_names, count, if with_values { 2 } else { 1 });
                if with_values {
                    for pair in flat_bytes(&reply).chunks_exact(2) {
                        assert!(
                            fields
                                .iter()
                                .any(|(field, value)| field == &pair[0] && value == &pair[1])
                        );
                    }
                }
            }
        }
        assert_eq!(
            input_state(cache),
            before,
            "reads changed input sequence or values"
        );
    }
}

#[test]
fn getrange_in_exec_and_lua_uses_replicated_clock_after_restore() {
    use crate::protocol::string::getrange::GetRangeParams;

    for proto in [2, 3] {
        let caches = replicas();
        for (clock, expected) in [(4_000, b"alu".as_slice()), (10_000, b"".as_slice())] {
            let mut eval = EvalParams::new(
                "local value = redis.call('GETRANGE', KEYS[1], 1, -2); redis.call('SET', KEYS[2], value); return {value, redis.call('GETRANGE', KEYS[3], 0, -1)}".into(),
                3,
                vec!["input:\0".into(), "saved:range".into(), "input:expired".into()],
                Vec::new(),
            );
            eval.proto = proto;
            let operations = vec![
                QueuedOperation::new(
                    0,
                    Operation::Read(ReadOperation::GetRange(GetRangeParams {
                        key: "input:\0".into(),
                        start: 1,
                        end: -2,
                    })),
                ),
                QueuedOperation::new(0, Operation::Redis(RedisOperation::RedisEval(eval))),
            ];
            let log = bincode2::serialize(&Request::new(
                clock,
                0,
                Operation::Redis(RedisOperation::RedisExec(ExecParams { operations })),
            ))
            .unwrap();
            let expected_value = Value::BulkString(Some(Bytes::copy_from_slice(expected)));
            let expected_reply = Value::Array(Some(vec![
                expected_value.clone(),
                Value::Array(Some(vec![
                    expected_value.clone(),
                    Value::BulkString(Some(Bytes::new())),
                ])),
            ]));
            for cache in &caches {
                assert_eq!(
                    apply_log(cache, &log).encode_proto(proto),
                    expected_reply.encode_proto(proto)
                );
                assert_eq!(
                    cache
                        .execute_read(
                            GetParams {
                                key: "saved:range".into(),
                            },
                            0,
                            clock,
                        )
                        .encode_proto(proto),
                    expected_value.encode_proto(proto)
                );
            }
        }
    }
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
    let all_members = dense_members(cache, "input:bag");
    let other_members = dense_members(cache, "input:other");
    let all_fields = dense_fields(cache);
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
        } else if label.starts_with('h') && matches!(*label, "hrandom" | "hunique" | "hrepeated") {
            let field_names = all_fields
                .iter()
                .map(|(field, _)| field.clone())
                .collect::<Vec<_>>();
            let count = match *label {
                "hrandom" => None,
                "hunique" => Some(3),
                _ => Some(-16),
            };
            let with_values = *label != "hrandom";
            assert_sample(
                &Value::Array(Some(
                    values
                        .iter()
                        .cloned()
                        .map(|value| Value::BulkString(Some(value)))
                        .collect(),
                )),
                &field_names,
                count,
                if with_values { 2 } else { 1 },
            );
            if with_values {
                for pair in values.chunks_exact(2) {
                    assert!(
                        all_fields
                            .iter()
                            .any(|(field, value)| field == &pair[0] && value == &pair[1])
                    );
                }
            }
        } else {
            let mut expected = match *label {
                "members" => all_members.clone(),
                "hash" => all_fields
                    .iter()
                    .flat_map(|(key, value)| [key.clone(), value.clone()])
                    .collect(),
                "fields" => all_fields.iter().map(|(key, _)| key.clone()).collect(),
                "values" => all_fields.iter().map(|(_, value)| value.clone()).collect(),
                "intersection" => all_members
                    .iter()
                    .filter(|member| other_members.contains(member))
                    .cloned()
                    .collect(),
                "union" => all_members
                    .iter()
                    .chain(&other_members)
                    .cloned()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect(),
                "difference" => all_members
                    .iter()
                    .filter(|member| !other_members.contains(member))
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
            // These commands still retain their canonical sorted Lua/EXEC path.
            if matches!(*label, "fields" | "intersection" | "union" | "difference") {
                expected.sort_unstable();
            }
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
                let before = input_state(cache);
                let reply = apply_log(cache, &log);
                assert_saved_results(cache, &prefix, &reply);
                assert_eq!(input_state(cache), before);
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
        ReadOperation::HRandField(HRandFieldParams {
            key: "input:hash".into(),
            count: None,
            with_values: false,
        }),
        ReadOperation::HRandField(HRandFieldParams {
            key: "input:hash".into(),
            count: Some(3),
            with_values: true,
        }),
        ReadOperation::HRandField(HRandFieldParams {
            key: "input:hash".into(),
            count: Some(-16),
            with_values: true,
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
    let before = caches.iter().map(input_state).collect::<Vec<_>>();
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
    assert_eq!(caches.iter().map(input_state).collect::<Vec<_>>(), before);
}

#[tokio::test]
async fn dense_reads_survive_disk_snapshot_and_hash_cas_replay_without_reordering() {
    use crate::protocol::set::{sadd::SAddReq, srem::SRemReq};
    use tokio::sync::Mutex as AsyncMutex;

    let source = Arc::new(replica(false, false));
    let full_view = Arc::new(MyCache::new(1).unwrap());
    full_view.pause_expire_workers();
    for key in [
        b"input:bag".as_slice(),
        b"input:other",
        b"input:hash",
        b"input:\0",
        b"input:\x80",
        b"input:\xff",
        b"input:expired",
    ] {
        let entry = source.databases[0].mocha.get_entry(key).unwrap();
        full_view.databases[0]
            .mocha
            .insert_snapshot(Bytes::copy_from_slice(key), entry);
    }
    let operations = [
        BaseOperation::HSet(HSetReq {
            key: "input:hash".into(),
            elements: vec![
                ("a".into(), "updated".into()),
                ("z".into(), "1".into()),
                ("z".into(), "2".into()),
            ],
        }),
        BaseOperation::SAdd(SAddReq {
            key: "input:bag".into(),
            elements: vec!["new-member".into(), "new-member".into()],
        }),
        BaseOperation::HDel(HDelReq {
            key: "input:hash".into(),
            fields: vec!["\0".into(), "\0".into()],
        }),
        BaseOperation::SRem(SRemReq {
            key: "input:bag".into(),
            members: vec![members()[5].clone()],
        }),
        BaseOperation::HSet(HSetReq {
            key: "input:hash".into(),
            elements: vec![("\0".into(), "restored".into())],
        }),
    ];
    let mut queue = Vec::new();
    let mut revision = 0;
    for (index, operation) in operations.into_iter().enumerate() {
        let mut update_type = UpdateType::Snapshot {
            queue: &mut queue,
            revision: &mut revision,
        };
        let mut update = Update {
            db_number: 0,
            write_clock: source.set_write_clock(4_000 + index as u64),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let reply = base_request(&source, operation, &mut update);
        assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        if index == 0 {
            // A valid concurrent full pass saw the hash after the first write,
            // but the Set before its writes. The first hash CAS must be skipped.
            let entry = source.databases[0]
                .mocha
                .get_entry(b"input:hash".as_slice())
                .unwrap();
            full_view.databases[0]
                .mocha
                .insert_snapshot("input:hash".into(), entry);
        }
    }
    assert_eq!(queue.len(), 5);
    full_view.set_write_clock(source.get_write_clock());
    let directory = tempfile::tempdir().unwrap();
    dump_cache_to_path(
        full_view,
        directory.path(),
        Arc::new(AsyncMutex::new(RaftMetaData {
            snapshot_state: SnapshotState::Start,
            snapshot_revision: revision,
            last_applied_log_id: None,
            last_membership: Default::default(),
        })),
        Arc::new(AsyncMutex::new(queue)),
    )
    .await
    .unwrap();
    let restored = Arc::new(MyCache::new(1).unwrap());
    restored.pause_expire_workers();
    let (_, queue, clock, _) = load_cache_from_path(
        restored.clone(),
        directory
            .path()
            .join("snapshot")
            .join(get_snapshot_file_name()),
    )
    .await
    .unwrap()
    .unwrap();
    for (index, delta) in queue.into_iter().enumerate() {
        let mut update_type = UpdateType::CAS {
            expected_revision: delta.expected_revision,
            revision: delta.version,
        };
        let mut update = Update {
            db_number: delta.db_number,
            write_clock: restored.set_write_clock(delta.write_clock),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let reply = base_request(&restored, delta.request, &mut update);
        if index == 0 {
            assert!(matches!(reply, Value::Null));
        } else {
            assert!(!matches!(reply, Value::Error(_)), "{reply:?}");
        }
    }
    restored.set_write_clock(clock);
    assert_eq!(input_state(&source), input_state(&restored));
    for read in [
        ReadOperation::SMembers(SMembersParams {
            key: "input:bag".into(),
        }),
        ReadOperation::HGetAll(HGetAllParams {
            key: "input:hash".into(),
        }),
        ReadOperation::HVals(HValsParams {
            key: "input:hash".into(),
        }),
    ] {
        assert_eq!(
            read_request(&source, read.clone(), 0, clock).encode(),
            read_request(&restored, read, 0, clock).encode()
        );
    }
    for proto in [2, 3] {
        let prefix = format!("restored:{proto}:");
        let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: vec![
                QueuedOperation::new(
                    0,
                    Operation::Read(ReadOperation::SMembers(SMembersParams {
                        key: "input:bag".into(),
                    })),
                ),
                QueuedOperation::new(
                    0,
                    Operation::Read(ReadOperation::HGetAll(HGetAllParams {
                        key: "input:hash".into(),
                    })),
                ),
                QueuedOperation::new(
                    0,
                    Operation::Read(ReadOperation::HVals(HValsParams {
                        key: "input:hash".into(),
                    })),
                ),
                QueuedOperation::new(0, eval(&prefix, "call", proto)),
            ],
        }));
        let log = bincode2::serialize(&Request::new(clock + 1, 0, operation)).unwrap();
        let before = input_state(&source);
        let replies = [&source, &restored].map(|cache| apply_log(cache, &log));
        assert_eq!(
            replies[0].encode_proto(proto),
            replies[1].encode_proto(proto)
        );
        for (cache, reply) in [&source, &restored].into_iter().zip(&replies) {
            let Value::Array(Some(values)) = reply else {
                panic!("expected EXEC reply: {reply:?}");
            };
            assert_saved_results(cache, &prefix, values.last().unwrap());
            assert_eq!(input_state(cache), before);
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
            Bytes::from_static(
                br#"
                local result = redis.call('HSCAN', KEYS[1], ARGV[1], 'COUNT', 1)
                redis.call('SET', KEYS[2], result[2][1])
                redis.call('HSET', KEYS[1], result[2][1], 'written')
                return result
            "#,
            ),
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
                    .execute_read(
                        GetParams {
                            key: "scan:first".into()
                        },
                        0,
                        4_000
                    )
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

#[test]
fn scan_cursor_from_ordinary_read_is_deterministic_in_exec_and_lua() {
    use crate::protocol::key::scan::ScanCommand;
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::mocha::request_handler::read_request;

    for proto in [2, 3] {
        let caches = replicas();
        for (index, cache) in caches.iter().enumerate() {
            let order: Vec<_> = if index == 0 {
                (0..128).collect()
            } else {
                (0..128).rev().collect()
            };
            for item in order {
                cache.databases[0].mocha.insert_persistent(
                    format!("scan:input:{item}").into(),
                    MyValue::new(ValueObject::Int(item)),
                );
            }
        }
        let arguments: Vec<_> = [
            "SCAN",
            "0",
            "MATCH",
            "scan:input:*",
            "TYPE",
            "string",
            "COUNT",
            "1",
        ]
        .into_iter()
        .map(|value| Value::BulkString(Some(value.into())))
        .collect();
        let Operation::Read(read) = ScanCommand.raft_request(&arguments).unwrap() else {
            panic!("SCAN must be a read");
        };
        let Value::Array(Some(page)) = read_request(&caches[0], read, 0, 4_000) else {
            panic!("SCAN must return an array");
        };
        let cursor = page[0].string_bytes_clone().unwrap();
        assert_ne!(cursor.as_ref(), b"0");
        let mut continuation = arguments;
        continuation[1] = Value::BulkString(Some(cursor.clone()));
        let scan = ScanCommand.raft_request(&continuation).unwrap();
        let mut script = EvalParams::new(
            Bytes::from_static(br#"
                local result = redis.call('SCAN', ARGV[1], 'COUNT', 1, 'MATCH', 'scan:input:*', 'TYPE', 'STRING')
                redis.call('SET', KEYS[1], result[2][1])
                redis.call('SET', result[2][1], 'written')
                return result
            "#),
            1,
            vec!["scan:selected".into()],
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
                    panic!("expected SCAN reply")
                };
                assert_eq!(page[0].string_bytes_clone().unwrap().as_ref(), b"0");
                let Value::Array(Some(keys)) = &page[1] else {
                    panic!("expected key array")
                };
                let keys: Vec<_> = keys
                    .iter()
                    .map(|key| key.string_bytes_clone().unwrap())
                    .collect();
                assert_eq!(keys.len(), 128);
                assert!(keys.is_sorted());
                assert_eq!(keys[0].as_ref(), b"scan:input:0");
            }
            assert_eq!(
                cache
                    .execute_read(
                        GetParams {
                            key: "scan:selected".into()
                        },
                        0,
                        4_000
                    )
                    .string_bytes_clone()
                    .unwrap()
                    .as_ref(),
                b"scan:input:0"
            );
            assert_eq!(
                cache
                    .execute_read(
                        GetParams {
                            key: "scan:input:0".into()
                        },
                        0,
                        4_000
                    )
                    .string_bytes_clone()
                    .unwrap()
                    .as_ref(),
                b"written"
            );
        }
    }
}
