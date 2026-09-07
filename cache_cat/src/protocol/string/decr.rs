use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::Decr;
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parameters for DECR command
#[derive(Debug, Clone, PartialEq)]
pub struct DecrParams {
    pub key: Bytes,
}

impl DecrParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("DECR"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(DecrParams { key })
    }
}

/// DECR command executor
pub struct DecrCommand;

impl RaftCommand for DecrCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(Decr(DecrReq {
            key: DecrParams::parse(items)?.key,
        })))
    }
}

#[async_trait]
impl Command for DecrCommand {
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
pub struct DecrReq {
    pub key: Bytes,
}

impl fmt::Display for DecrReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DecrReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ComputeCommand for DecrReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Decr(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (result, value) = match &entry.value.data {
            ValueObject::Int(n) => {
                let Some(num) = n.checked_sub(1) else {
                    return (MochaOperation::Abort, ProtocolError::Overflow.into());
                };
                (ValueObject::Int(num), Value::Integer(num))
            }

            ValueObject::String(s) => {
                let Some(value) = parse_i64(s) else {
                    return (MochaOperation::Abort, ProtocolError::NotAnInteger.into());
                };
                let Some(result) = value.checked_sub(1) else {
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
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Int(-1)), // 初始值为-1，符合DECR语义
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(-1), // 返回-1
        )
    }
}
