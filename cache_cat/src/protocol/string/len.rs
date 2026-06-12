use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for STRLEN command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrLenParams {
    pub key: Vec<u8>,
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

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(Self { key })
    }
}

/// STRLEN command executor
pub struct StrLenCommand;

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
            return Ok(Value::SimpleString("QUEUED".to_string()));
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
