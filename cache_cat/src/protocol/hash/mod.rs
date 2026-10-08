pub mod hdel;
pub mod hexists;
pub mod hget;
pub mod hgetall;
pub mod hincrby;
pub mod hkeys;
pub mod hlen;
pub mod hmget;
pub mod hmset;
pub mod hset;
pub mod hsetnx;
pub mod hvals;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProtocolError;
    use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::mocha::cas::ComputeCommand;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::mocha::read_command::ReadCommand;
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::value_object::{HashValue, ValueObject};
    use bytes::Bytes;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn hash_entry(
        fields: &[(Bytes, HashValue)],
        capacity: usize,
        reverse: bool,
    ) -> EntrySnapshot<MyValue> {
        let mut map = HashMap::with_capacity(capacity);
        for index in 0..fields.len() {
            let index = if reverse {
                fields.len() - index - 1
            } else {
                index
            };
            let (field, value) = &fields[index];
            map.insert(field.clone(), value.clone());
        }
        EntrySnapshot {
            value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
            expire_at: None,
        }
    }

    fn enumeration_commands() -> [Box<dyn ReadCommand>; 3] {
        [
            Box::new(hgetall::HGetAllParams { key: "hash".into() }),
            Box::new(hkeys::HKeysParams { key: "hash".into() }),
            Box::new(hvals::HValsParams { key: "hash".into() }),
        ]
    }

    #[test]
    fn clocked_hash_reads_are_identical_across_hash_table_layouts() {
        // Values deliberately have a different order from their fields and include duplicates.
        let ordered_fields = vec![
            (Bytes::from_static(b""), HashValue::Str("last".into())),
            (Bytes::from_static(b"\0"), HashValue::Int(-4)),
            (Bytes::from_static(b"a"), HashValue::Str("duplicate".into())),
            (Bytes::from_static(b"a\0"), HashValue::Str("middle".into())),
            (Bytes::from_static(b"z"), HashValue::Str("duplicate".into())),
            (
                Bytes::from_static(b"\x80"),
                HashValue::Str(Bytes::from_static(b"\xfe")),
            ),
            (Bytes::from_static(b"\xff"), HashValue::Str("first".into())),
        ];
        let expected = [
            Value::Map(
                ordered_fields
                    .iter()
                    .map(|(field, value)| {
                        (
                            Value::BulkString(Some(field.clone())),
                            Value::BulkString(Some(value.to_bytes())),
                        )
                    })
                    .collect(),
            ),
            Value::Array(Some(
                ordered_fields
                    .iter()
                    .map(|(field, _)| Value::BulkString(Some(field.clone())))
                    .collect(),
            )),
            Value::Array(Some(
                ordered_fields
                    .iter()
                    .map(|(_, value)| Value::BulkString(Some(value.to_bytes())))
                    .collect(),
            )),
        ];
        let commands = enumeration_commands();
        for (capacity, reverse) in [(0, false), (32, true), (1024, false)] {
            let hash = hash_entry(&ordered_fields, capacity, reverse);
            for clock in [0, 123_456] {
                for (command, expected) in commands.iter().zip(&expected) {
                    let reply = command.execute_with_clock(Some(hash.clone()), clock);
                    for proto in [2, 3] {
                        assert_eq!(reply.encode_proto(proto), expected.encode_proto(proto));
                    }
                }
            }
        }
    }

    #[test]
    fn clocked_hash_reads_preserve_empty_replies_and_wrong_type_errors() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String("value".into())),
            expire_at: None,
        };
        let empty_hash = hash_entry(&[], 0, false);
        let expected_empty = [
            Value::Map(vec![]),
            Value::Array(Some(vec![])),
            Value::Array(Some(vec![])),
        ];
        for (command, expected) in enumeration_commands().iter().zip(expected_empty) {
            for proto in [2, 3] {
                for entry in [None, Some(empty_hash.clone())] {
                    assert_eq!(
                        command.execute_with_clock(entry, 17).encode_proto(proto),
                        expected.encode_proto(proto)
                    );
                }
                assert_eq!(
                    command
                        .execute_with_clock(Some(wrong_type.clone()), 17)
                        .encode_proto(proto),
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                );
            }
        }
    }

    fn entry(operation: MochaOperation<MyValue>) -> EntrySnapshot<MyValue> {
        let MochaOperation::Insert { value, .. } = operation else {
            panic!("expected hash insertion");
        };
        EntrySnapshot {
            value,
            expire_at: Some(10_000),
        }
    }

    #[test]
    fn duplicate_fields_count_once_and_deleting_last_field_removes_key() {
        let (operation, reply) = hset::HSetReq {
            key: Bytes::from_static(b"h"),
            elements: vec![("f".into(), "1".into()), ("f".into(), "2".into())],
        }
        .init();
        assert_eq!(reply.encode(), b":1\r\n");
        let hash = entry(operation);
        assert_eq!(
            hget::HGetParams {
                key: "h".into(),
                field: "f".into()
            }
            .execute(Some(hash.clone()))
            .encode(),
            b"$1\r\n2\r\n"
        );
        let (operation, reply) = hdel::HDelReq {
            key: "h".into(),
            fields: vec!["f".into(), "f".into()],
        }
        .mutate(hash, 0);
        assert!(matches!(operation, MochaOperation::Remove));
        assert_eq!(reply.encode(), b":1\r\n");
    }

    #[test]
    fn missing_hash_returns_a_null_for_each_requested_field() {
        let reply = hmget::HMGetParams {
            key: "missing".into(),
            fields: vec!["a".into(), "b".into(), "a".into()],
        }
        .execute(None);
        assert_eq!(reply.encode(), b"*3\r\n$-1\r\n$-1\r\n$-1\r\n");
    }

    #[test]
    fn hincrby_accepts_hmset_integers_preserves_ttl_and_aborts_on_overflow() {
        for (old, increment, expected) in [
            ("41", 1, Some(42)),
            ("9223372036854775807", 1, None),
            ("-9223372036854775808", -1, None),
        ] {
            let (operation, _) = hmset::HMSetReq {
                key: "h".into(),
                fields: vec![("f".into(), Bytes::copy_from_slice(old.as_bytes()))],
            }
            .init();
            let hash = entry(operation);
            let (operation, reply) = hincrby::HIncrReq {
                key: "h".into(),
                field: "f".into(),
                value: increment,
            }
            .mutate(hash.clone(), 0);
            match expected {
                Some(value) => {
                    assert!(matches!(
                        operation,
                        MochaOperation::Insert {
                            expire: ExpirePolicy::Absolute(10_000),
                            ..
                        }
                    ));
                    assert!(matches!(reply, Value::Integer(n) if n == value));
                }
                None => {
                    assert!(matches!(operation, MochaOperation::Abort));
                    assert_eq!(
                        reply.encode(),
                        b"-ERR increment or decrement would overflow\r\n"
                    );
                    assert_eq!(
                        hget::HGetParams {
                            key: "h".into(),
                            field: "f".into()
                        }
                        .execute(Some(hash))
                        .encode(),
                        Value::BulkString(Some(Bytes::copy_from_slice(old.as_bytes()))).encode()
                    );
                }
            }
        }
    }

    #[test]
    fn hincrby_rejects_noncanonical_hmset_values_without_changing_them() {
        for old in ["01", "+1", "-0", " 1 ", "1.0", "9223372036854775808"] {
            let (operation, _) = hmset::HMSetReq {
                key: "h".into(),
                fields: vec![("f".into(), Bytes::copy_from_slice(old.as_bytes()))],
            }
            .init();
            let hash = entry(operation);
            let (operation, reply) = hincrby::HIncrReq {
                key: "h".into(),
                field: "f".into(),
                value: 1,
            }
            .mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(reply.encode(), b"-ERR hash value is not an integer\r\n");
            assert_eq!(
                hget::HGetParams {
                    key: "h".into(),
                    field: "f".into()
                }
                .execute(Some(hash))
                .encode(),
                Value::BulkString(Some(Bytes::copy_from_slice(old.as_bytes()))).encode()
            );
        }
    }

    #[test]
    fn hincrby_requires_a_canonical_increment() {
        for increment in ["+1", "01", "-0", " 1 "] {
            let args = [
                Value::BulkString(Some("HINCRBY".into())),
                Value::BulkString(Some("h".into())),
                Value::BulkString(Some("f".into())),
                Value::BulkString(Some(increment.into())),
            ];
            assert!(
                matches!(
                    hincrby::HIncrByCommand.raft_request(&args),
                    Err(ProtocolError::NotAnInteger)
                ),
                "increment {increment:?}"
            );
        }
    }

    #[test]
    fn fixed_arity_hash_commands_reject_extra_arguments() {
        let commands: [(&dyn RaftCommand, usize); 6] = [
            (&hget::HGetCommand, 3),
            (&hexists::HExistsCommand, 3),
            (&hgetall::HGetAllCommand, 2),
            (&hkeys::HKeysCommand, 2),
            (&hlen::HLenCommand, 2),
            (&hvals::HValsCommand, 2),
        ];
        for (command, arity) in commands {
            let mut args = vec![Value::BulkString(Some("arg".into())); arity];
            assert!(command.raft_request(&args).is_ok());
            args.push(Value::BulkString(Some("extra".into())));
            assert!(matches!(
                command.raft_request(&args),
                Err(ProtocolError::WrongArgCount(_))
            ));
        }
    }
}
