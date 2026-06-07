//! RENAMENX command implementation
//!
//! RENAMENX key newkey
//!
//! Renames `key` to `newkey` if `newkey` does not yet exist.
//! Returns 1 if the key was renamed, 0 if `newkey` already exists.
//! Returns an error if the source key does not exist.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// RENAMENX command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameNxParams {
    pub key: Vec<u8>,
    pub new_key: Vec<u8>,
}

impl RenameNxParams {
    /// Parse RENAMENX command parameters from RESP array items
    /// Format: RENAMENX key newkey
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("renamenx"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("renamenx")),
        };

        let new_key = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("renamenx")),
        };

        Ok(RenameNxParams { key, new_key })
    }
}

impl Display for RenameNxParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RENAMENX {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.new_key)
        )
    }
}

/// RENAMENX command executor
pub struct RenameNxCommand;

impl RaftCommand for RenameNxCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisRenameNx(
            RenameNxParams::parse(items)?,
        )))
    }
}

#[async_trait]
impl Command for RenameNxCommand {
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