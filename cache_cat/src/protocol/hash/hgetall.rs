//! HGETALL command implementation
//!
//! HGETALL key
//! Returns all fields and values of the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HGETALL arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HGetAllParams {
    pub key: Bytes,
}

impl Display for HGetAllParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HGETALL {}", String::from_utf8_lossy(&self.key))
    }
}

/// HGETALL command handler
pub struct HGetAllCommand;

impl HGetAllCommand {
    /// Parse arguments from RESP items
    /// Format: HGETALL key
    fn parse_args(items: &[Value]) -> Result<HGetAllParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hgetall"));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(HGetAllParams { key: key.into() })
    }
}

impl ReadRaftCommand for HGetAllCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HGetAll(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HGetAllCommand {
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
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
