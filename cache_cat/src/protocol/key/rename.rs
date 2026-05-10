//! RENAME command implementation
//!
//! RENAME key newkey
//!
//! Renames `key` to `newkey`. If `newkey` already exists it is overwritten.
//! Returns an error if the source key does not exist.

use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Expire;
use crate::raft::types::entry::request::Request::Redis;
use crate::raft::types::entry::request::{RedisOperation, Request};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU16;

/// RENAME command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameParams {
    pub key: Vec<u8>,
    pub new_key: Vec<u8>,
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

        Ok(RenameParams { key, new_key })
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

#[async_trait]
impl Command for RenameCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = RenameParams::parse(items)?;
        let value = server
            .app
            .write_redis(RedisOperation::RedisRename(params), *db_number)
            .await?;
        Ok(value)
    }
}
