//! PTTL command implementation
//!
//! PTTL key
//! Returns the remaining time to live of a key that has a timeout,
//! in milliseconds.
//!
//! Return value:
//! - Integer: TTL in milliseconds
//! - Integer: -1 if key exists but has no associated expire
//! - Integer: -2 if key does not exist

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::now_ms;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// PTTL command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PTtlCommand;

/// Parsed arguments for PTTL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PTtlParams {
    pub key: Bytes,
}

impl Display for PTtlParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PttlParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ReadCommand for PTtlParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // Key does not exist
            None => Value::Integer(-2),

            Some(entry) => {
                match entry.expire_at {
                    // Key exists but has no associated expire
                    None => Value::Integer(-1),

                    // Key exists and has an expire time
                    Some(expire_at) => {
                        // Get current time in milliseconds
                        let now = now_ms();
                        // Calculate remaining TTL in milliseconds
                        let ttl = (expire_at - now) as i64;
                        Value::Integer(ttl)
                    }
                }
            }
        }
    }
}

impl PTtlCommand {
    /// Parse PTTL arguments: PTTL key
    fn parse_args(items: &[Value]) -> Result<PTtlParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("pttl"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(PTtlParams { key })
    }
}

impl ReadRaftCommand for PTtlCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::PTtl(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for PTtlCommand {
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
        server.app.read(params, client.db_number).await
    }
}
