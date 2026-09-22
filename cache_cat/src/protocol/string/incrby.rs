use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation::{self, IncrBy};
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_canonical_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parameters for INCR command
#[derive(Debug, Clone, PartialEq)]
pub struct IncrByParams {
    pub key: Bytes,
    pub increment: i64,
}

impl IncrByParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("incrby"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // getLongLongFromObjectOrReply: the argument must be a canonical integer.
        let increment = items[2].try_parse_canonical_i64()?;

        Ok(IncrByParams { key, increment })
    }
}

/// INCR command executor
pub struct IncrByCommand;

impl RaftCommand for IncrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = IncrByParams::parse(items)?;
        Ok(Operation::Base(IncrBy(IncrByReq {
            key: params.key,
            increment: params.increment,
        })))
    }
}

#[async_trait]
impl Command for IncrByCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncrByReq {
    pub key: Bytes,
    pub increment: i64,
}

impl fmt::Display for IncrByReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncrByReq {{ key: {}, increment: {} }}",
            String::from_utf8_lossy(&self.key),
            self.increment
        )
    }
}

impl ComputeCommand for IncrByReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::IncrBy(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (result, value) = match &entry.value.data {
            ValueObject::Int(n) => {
                let Some(num) = n.checked_add(self.increment) else {
                    return (MochaOperation::Abort, ProtocolError::Overflow.into());
                };
                (ValueObject::Int(num), Value::Integer(num))
            }

            ValueObject::String(s) => {
                let Some(value) = parse_canonical_i64(s) else {
                    return (MochaOperation::Abort, ProtocolError::NotAnInteger.into());
                };
                let Some(result) = value.checked_add(self.increment) else {
                    return (MochaOperation::Abort, ProtocolError::Overflow.into());
                };
                (ValueObject::Int(result), Value::Integer(result))
            }

            _ => {
                return (MochaOperation::Abort, ProtocolError::WrongType.into());
            }
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(result),
                expire: entry.get_expire_policy(),
            },
            value,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let v = self.increment;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Int(v)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(v),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(amount: &'static [u8]) -> [Value; 3] {
        let bulk = |v: &'static [u8]| Value::BulkString(Some(Bytes::from_static(v)));
        [bulk(b"INCRBY"), bulk(b"key"), bulk(amount)]
    }

    #[test]
    fn amount_must_be_a_canonical_integer() {
        // Redis getLongLongFromObjectOrReply -> string2ll.
        let rejected: [&'static [u8]; 10] = [
            b"+5",
            b"05",
            b"-0",
            b"-05",
            b" 5",
            b"5 ",
            b"",
            b"1.5",
            b"abc",
            b"9223372036854775808",
        ];
        for amount in rejected {
            assert_eq!(
                IncrByParams::parse(&args(amount)),
                Err(ProtocolError::NotAnInteger),
                "{:?}",
                amount
            );
        }

        let accepted: [(&'static [u8], i64); 4] = [
            (b"0", 0),
            (b"5", 5),
            (b"-5", -5),
            (b"9223372036854775807", i64::MAX),
        ];
        for (amount, expected) in accepted {
            let params = IncrByParams::parse(&args(amount)).expect("canonical integer");
            assert_eq!(params.increment, expected);
        }
    }
}
