//! SUBSCRIBE command implementation
//!
//! SUBSCRIBE channel [channel ...]
//! Subscribes the client to the specified channels.
//!
//! Once the client enters the subscribed state, it is not supposed to
//! issue any other commands, except for additional SUBSCRIBE, PSUBSCRIBE,
//! UNSUBSCRIBE, PUNSUBSCRIBE, PING, RESET and QUIT commands.
//!
//! Returns:
//! - For each channel subscribed: a multi-bulk reply with three elements:
//!   - "subscribe"
//!   - channel name
//!   - number of channels the client is currently subscribed to

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{BlockCommand, Client};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::sync::watch;

/// SUBSCRIBE command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeParams {
    pub channels: Vec<Vec<u8>>,
}

impl SubscribeParams {
    /// Parse SUBSCRIBE command parameters from RESP array items
    /// Format: SUBSCRIBE channel [channel ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: SUBSCRIBE channel (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("subscribe"));
        }

        let mut channels = Vec::with_capacity(items.len() - 1);

        // Parse all channel arguments
        for item in &items[1..] {
            let channel = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::WrongArgCount("subscribe")),
            };
            channels.push(channel);
        }

        Ok(SubscribeParams { channels })
    }
}

impl Display for SubscribeParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SubscribeReq {{ channels: {:?} }}", self.channels)
    }
}

/// SUBSCRIBE command executor
pub struct SubscribeCommand;

#[async_trait]
impl BlockCommand for SubscribeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<(Value, watch::Receiver<Option<Value>>), CacheCatError> {
        let params = SubscribeParams::parse(items)?;
        Ok(server.broadcast.subscribe(params.channels, client.id).await)
    }

    fn can_handle_unblock(&self, cmd_name: &str) -> bool {
        true
    }
}
