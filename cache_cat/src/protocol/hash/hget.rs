//! HGET command implementation
//!
//! HGET key field
//! Returns the value associated with field in the hash stored at key.

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

/// Parsed HGET arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HGetParams {
    pub key: Bytes,
    pub field: Vec<u8>,
}

impl Display for HGetParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HGET {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field)
        )
    }
}

/// HGET command handler
pub struct HGetCommand;

impl HGetCommand {
    /// Parse arguments from RESP items
    /// Format: HGET key field
    fn parse_args(items: &[Value]) -> Result<HGetParams, ProtocolError> {
        // HGET key field (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hget"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse field
        let field = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("field")),
        };

        Ok(HGetParams {
            key: key.into(),
            field,
        })
    }
}

impl ReadRaftCommand for HGetCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HGet(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HGetCommand {
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
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}
