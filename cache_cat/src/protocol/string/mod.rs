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
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::response_value::Value;

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
}
