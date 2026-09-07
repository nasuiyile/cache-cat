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

pub struct LLenCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLenParams {
    pub key: Bytes,
}

impl Display for LLenParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LLenParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl LLenCommand {
    fn parse_args(items: &[Value]) -> Result<LLenParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("llen"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(LLenParams { key })
    }
}

impl ReadCommand for LLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Integer(0),
            Some(v) => match v.value.data {
                ValueObject::List(list) => Value::Integer(list.lock().len() as i64),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl ReadRaftCommand for LLenCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::LLen(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for LLenCommand {
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
