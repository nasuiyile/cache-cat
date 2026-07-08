use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation::{self, IncrBy};
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
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
            return Err(ProtocolError::WrongArgCount("INCR"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let increment = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("increment"))?;

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
            return Ok(Value::SimpleString(String::from("QUEUED")));
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
                let num = n + self.increment;
                (ValueObject::Int(num), Value::Integer(num))
            }

            ValueObject::String(s) => {
                let Some(mut value) = parse_i64(s) else {
                    return (
                        MochaOperation::Abort,
                        Value::Error("Value is not an integer".to_string()),
                    );
                };
                value += self.increment;
                (ValueObject::Int(value), Value::Integer(value))
            }

            _ => {
                return (
                    MochaOperation::Abort,
                    Value::Error("Key exists but is not an Integer".to_string()),
                );
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
