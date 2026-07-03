use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::Incr;
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parameters for INCR command
#[derive(Debug, Clone, PartialEq)]
pub struct IncrParams {
    pub key: Bytes,
}

impl IncrParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("INCR"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(IncrParams { key })
    }
}

/// INCR command executor
pub struct IncrCommand;

impl RaftCommand for IncrCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(Incr(IncrReq {
            key: IncrParams::parse(items)?.key,
            value: 1,
        })))
    }
}

#[async_trait]
impl Command for IncrCommand {
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
pub struct IncrReq {
    pub key: Bytes,
    pub value: i64,
}

impl fmt::Display for IncrReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncrReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ComputeCommand for IncrReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Incr(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (result, value) = match &entry.value.data {
            ValueObject::Int(n) => {
                let num = n + self.value;
                (ValueObject::Int(num), Value::Integer(num))
            }
            ValueObject::String(s) => {
                if let Some(v) = parse_i64(s) {
                    let new_val = v + self.value;
                    (ValueObject::Int(new_val), Value::Integer(new_val))
                } else {
                    return (
                        MochaOperation::Abort,
                        Value::Error("Value is not an integer".to_string()),
                    );
                }
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
        let v = self.value;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Int(v)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(v),
        )
    }
}
