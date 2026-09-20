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
    use bytes::Bytes;

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
