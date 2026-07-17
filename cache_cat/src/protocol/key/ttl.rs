//! TTL command implementation
//!
//! TTL key
//! Returns the remaining time to live of a key that has a timeout,
//! in seconds.
//!
//! Return value:
//! - Integer: TTL in seconds
//! - Integer: -1 if key exists but has no associated expire
//! - Integer: -2 if key does not exist

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::now_ms;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// TTL command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlCommand;

/// Parsed arguments for TTL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlParams {
    pub key: Bytes,
}

impl Display for TtlParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TtlParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ReadCommand for TtlParams {
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
                        let ttl_ms = expire_at - now;

                        // Convert milliseconds to seconds, rounding up
                        // If TTL is positive but less than 1 second, return 1
                        // to match Redis behavior
                        let ttl_sec = ttl_ms.div_ceil(1000) as i64;

                        Value::Integer(ttl_sec)
                    }
                }
            }
        }
    }
}

impl TtlCommand {
    /// Parse TTL arguments: TTL key
    fn parse_args(items: &[Value]) -> Result<TtlParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("ttl"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(TtlParams { key })
    }
}

impl ReadRaftCommand for TtlCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Ttl(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for TtlCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
