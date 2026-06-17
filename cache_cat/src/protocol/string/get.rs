use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for GET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetParams {
    pub key: Bytes,
}

impl Display for GetParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GET {}", String::from_utf8_lossy(&self.key))
    }
}

impl GetParams {
    /// Parse GET command parameters from RESP array items
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("GET"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        Ok(GetParams { key })
    }
}

/// GET command executor
pub struct GetCommand;

impl ReadRaftCommand for GetCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Get(GetParams::parse(items)?))
    }
}

#[async_trait]
impl Command for GetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }

        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
