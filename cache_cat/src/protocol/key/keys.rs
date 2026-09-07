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

/// Parameters for KEYS command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysParams {
    pub pattern: Bytes,
}

impl Display for KeysParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KEYS {}", String::from_utf8_lossy(&self.pattern))
    }
}

impl KeysParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("KEYS"));
        }

        let pattern = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("pattern"))?;

        Ok(Self { pattern })
    }
}

/// KEYS command executor
pub struct KeysCommand;

impl ReadRaftCommand for KeysCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Keys(KeysParams::parse(items)?))
    }
}

#[async_trait]
impl Command for KeysCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}
