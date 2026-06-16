//! RENAME command implementation
//!
//! RENAME key newkey
//!
//! Renames `key` to `newkey`. If `newkey` already exists it is overwritten.
//! Returns an error if the source key does not exist.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// RENAME command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameParams {
    pub key: Bytes,
    pub new_key: Bytes,
}

impl RenameParams {
    /// Parse RENAME command parameters from RESP array items
    /// Format: RENAME key newkey
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("rename"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("rename")),
        };

        let new_key = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("rename")),
        };

        Ok(RenameParams {
            key: key.into(),
            new_key: new_key.into(),
        })
    }
}
impl Display for RenameParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Insert {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.new_key)
        )
    }
}

/// RENAME command executor
pub struct RenameCommand;

impl RaftCommand for RenameCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisRename(
            RenameParams::parse(items)?,
        )))
    }
}

#[async_trait]
impl Command for RenameCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
