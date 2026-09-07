use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::mocha::EntrySnapshot;

/// Parameters for STRLEN command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrLenParams {
    pub key: Bytes,
}

impl Display for StrLenParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "STRLEN {}", String::from_utf8_lossy(&self.key))
    }
}

impl StrLenParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("STRLEN"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(Self { key })
    }
}

/// STRLEN command executor
pub struct StrLenCommand;

impl ReadCommand for StrLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let len = match value {
            None => 0,
            Some(v) => match v.value.data {
                ValueObject::String(ref bytes) => bytes.len(),
                ValueObject::Int(ref i) => i.to_string().len(),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        Value::Integer(len as i64)
    }
}

impl ReadRaftCommand for StrLenCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::StrLen(StrLenParams::parse(items)?))
    }
}

#[async_trait]
impl Command for StrLenCommand {
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
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
