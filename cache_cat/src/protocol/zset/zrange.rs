//! ZRANGE command implementation
//!
//! ZRANGE key start stop [WITHSCORES]
//! Returns the specified range of elements in the sorted set stored at key.
//! Elements are ordered from the lowest to the highest score.
//!
//! Start and stop are 0-based indices. Negative indices count from the end.
//! WITHSCORES returns member-score pairs.
//!
//! Return value:
//! - Array of members (or member, score, member, score, ... with WITHSCORES)
//! - Empty array if key does not exist
//! - WRONGTYPE error if key exists but is not a sorted set

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// ZRANGE command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeCommand;

/// Parsed arguments for ZRANGE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeParams {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
    pub with_scores: bool,
}

impl Display for ZRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRangeParams {{ key: {}, start: {}, stop: {}, with_scores: {} }}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.stop,
            self.with_scores
        )
    }
}

impl ZRangeCommand {
    /// Parse ZRANGE arguments: ZRANGE key start stop [WITHSCORES]
    fn parse_args(items: &[Value]) -> Result<ZRangeParams, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zrange"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let start = items[2].try_parse_i64()?;
        let stop = items[3].try_parse_i64()?;

        // Check for WITHSCORES flag
        let mut with_scores = false;
        if items.len() > 4 {
            for item in &items[4..] {
                let Some(flag) = item.as_str_lossy() else {
                    continue;
                };
                if flag.to_uppercase() == "WITHSCORES" {
                    with_scores = true;
                }
            }
        }

        Ok(ZRangeParams {
            key,
            start,
            stop,
            with_scores,
        })
    }
}

impl ReadRaftCommand for ZRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZRange(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZRangeCommand {
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
