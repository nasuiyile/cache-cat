pub mod append;
pub mod decr;
pub mod decrby;
pub mod fadd;
pub mod get;
pub mod getset;
pub mod incr;
pub mod incrby;
pub mod len;
pub mod mget;
pub mod mset;
pub mod pfcount;
pub mod pfmerge;
pub mod psetex;
pub mod set;
pub mod setex;
pub mod setnx;

#[cfg(test)]
mod tests {
    use super::{decrby::DecrByCommand, incrby::IncrByCommand};
    use crate::error::ProtocolError;
    use crate::mocha::{EntrySnapshot, MochaOperation};
    use crate::protocol::raft_command::RaftCommand;
    use crate::protocol::string::decr::DecrReq;
    use crate::protocol::string::decrby::DecrByReq;
    use crate::protocol::string::incr::IncrReq;
    use crate::protocol::string::incrby::IncrByReq;
    use crate::raft::types::core::mocha::cas::ComputeCommand;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::value_object::ValueObject;

    #[test]
    fn invalid_increments_use_the_redis_integer_error() {
        for command in [&IncrByCommand as &dyn RaftCommand, &DecrByCommand] {
            for amount in ["not-an-integer", "9223372036854775808"] {
                let args = [
                    Value::BulkString(Some("command".into())),
                    Value::BulkString(Some("key".into())),
                    Value::BulkString(Some(amount.into())),
                ];
                assert!(matches!(
                    command.raft_request(&args),
                    Err(ProtocolError::NotAnInteger)
                ));
            }
        }
    }

    fn integer_entry(value: i64) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Int(value)),
            expire_at: Some(123),
        }
    }

    fn assert_overflow(result: (MochaOperation<MyValue>, Value)) {
        assert!(matches!(result.0, MochaOperation::Abort));
        assert_eq!(
            result.1.encode(),
            b"-ERR increment or decrement would overflow\r\n"
        );
    }

    #[test]
    fn integer_overflow_aborts_string_increment_commands() {
        assert_overflow(IncrReq { key: "key".into() }.mutate(integer_entry(i64::MAX), 0));
        assert_overflow(DecrReq { key: "key".into() }.mutate(integer_entry(i64::MIN), 0));
        assert_overflow(
            IncrByReq {
                key: "key".into(),
                increment: 1,
            }
            .mutate(integer_entry(i64::MAX), 0),
        );
        assert_overflow(
            DecrByReq {
                key: "key".into(),
                decrement: 1,
            }
            .mutate(integer_entry(i64::MIN), 0),
        );
    }
}
