//! BLPOP command implementation
//!
//! BLPOP key [key ...] timeout
//! Remove and get the first element in a list, or block until one is available.
//!
//! When timeout is 0, it blocks indefinitely.
//! Returns:
//! - Two-element array: the key from which the element was popped and the value.
//! - nil when timeout is reached and no element could be popped.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{BlockCommand, Client, ParsedCommand};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisBLPop;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::sync::watch;

/// BLPOP command handler
pub struct BLPopCommand;

impl BLPopCommand {
    /// Parse arguments from RESP items
    /// Format: BLPOP key [key ...] timeout
    fn parse_args(items: &[Value]) -> Result<BLPopParams, ProtocolError> {
        // Minimum: BLPOP key timeout (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("blpop"));
        }

        // Parse keys (all args except the last one, which is timeout)
        let mut keys = Vec::with_capacity(items.len() - 2);
        for item in &items[1..items.len() - 1] {
            let key = item
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("key"))?
                .clone();

            keys.push(key);
        }

        // Parse timeout (last argument)
        let timeout = items[items.len() - 1]
            .as_str_lossy()
            .and_then(|str| str.parse::<f64>().ok())
            .ok_or(ProtocolError::InvalidArgument("timeout"))?;

        // Validate timeout
        if timeout < 0.0 {
            return Err(ProtocolError::InvalidArgument(
                "timeout must be non-negative",
            ));
        }

        Ok(BLPopParams { keys, timeout })
    }
}

/// Parsed BLPOP arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BLPopParams {
    pub keys: Vec<Bytes>,
    pub timeout: f64,
}

impl Display for BLPopParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BLPOP {}", self.timeout)
    }
}

impl RaftCommand for BLPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BLPopCommand::parse_args(items)?;
        Ok(Operation::Redis(RedisBLPop(params)))
    }
}

#[async_trait]
impl BlockCommand for BLPopCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<(Value, watch::Receiver<Option<Value>>), CacheCatError> {
        let _params = BLPopCommand::parse_args(items)?;

        todo!()
    }

    async fn execute_during_block(
        &self,
        _client: &mut Client,
        _cmd: &ParsedCommand,
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        todo!()
    }
}
