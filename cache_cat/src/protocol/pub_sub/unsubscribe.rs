//! UNSUBSCRIBE command implementation
//!
//! UNSUBSCRIBE [channel [channel ...]]
//! Unsubscribes the client from the given channels, or from all of
//! them if none is given.
//!
//! When no channels are specified, the client is unsubscribed from all
//! the previously subscribed channels. In this case, a message for every
//! unsubscribed channel will be sent to the client.
//!
//! Returns:
//! - For each channel unsubscribed: a multi-bulk reply with three elements:
//!   - "unsubscribe"
//!   - channel name
//!   - number of channels the client is currently subscribed to

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{BlockCommand, Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::sync::watch;

/// UNSUBSCRIBE command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnsubscribeParams {
    /// Channels to unsubscribe from.
    /// None means unsubscribe from all channels.
    pub channels: Option<Vec<Vec<u8>>>,
}

impl UnsubscribeParams {
    /// Parse UNSUBSCRIBE command parameters from RESP array items
    /// Format: UNSUBSCRIBE [channel [channel ...]]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        match items.len() {
            // UNSUBSCRIBE with no arguments - unsubscribe from all
            1 => Ok(UnsubscribeParams { channels: None }),
            // UNSUBSCRIBE channel [channel ...]
            _ => {
                let mut channels = Vec::with_capacity(items.len() - 1);

                for item in &items[1..] {
                    let channel = match item {
                        Value::BulkString(Some(data)) => data.clone(),
                        Value::SimpleString(s) => s.as_bytes().to_vec(),
                        _ => return Err(ProtocolError::WrongArgCount("unsubscribe")),
                    };
                    channels.push(channel);
                }

                Ok(UnsubscribeParams {
                    channels: Some(channels),
                })
            }
        }
    }
}

impl Display for UnsubscribeParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.channels {
            Some(channels) => write!(f, "UnsubscribeReq {{ channels: {:?} }}", channels),
            None => write!(f, "UnsubscribeReq {{ channels: all }}"),
        }
    }
}

/// UNSUBSCRIBE command executor
pub struct UnsubscribeCommand;

#[async_trait]
impl Command for UnsubscribeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = UnsubscribeParams::parse(items)?;
        let result = match params.channels {
            None => server.broadcast.unsubscribe_all_channels(client.id).await,
            Some(channels) => server.broadcast.unsubscribe(channels, client.id).await,
        };
        Ok(result)
    }
}
