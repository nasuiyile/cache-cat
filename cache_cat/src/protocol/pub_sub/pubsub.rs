//! PUBSUB command implementation
//!
//! PUBSUB subcommand [argument [argument ...]]
//!
//! Subcommands:
//! - PUBSUB CHANNELS [pattern] - List active channels (optionally matching pattern)
//! - PUBSUB NUMSUB [channel-1 ... channel-N] - Return number of subscribers for channels
//! - PUBSUB NUMPAT - Return number of pattern subscriptions
//!
//! Returns:
//! - For CHANNELS: Array of channel names
//! - For NUMSUB: Array of channel/subscriber-count pairs
//! - For NUMPAT: Integer count

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::{Display, Formatter};

/// PUBSUB subcommand types
#[derive(Debug, Clone, PartialEq)]
pub enum PubSubSubCommand {
    /// CHANNELS [pattern] - List active channels matching pattern (or all if pattern not provided)
    Channels(Option<Bytes>),
    /// NUMSUB [channel ...] - Get number of subscribers for specified channels
    NumSub(Vec<Bytes>),
    /// NUMPAT - Get number of pattern subscriptions
    NumPat,
}

/// PUBSUB command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PubSubParams {
    pub subcommand: PubSubSubCommand,
}

impl PubSubParams {
    /// Parse PUBSUB command parameters from RESP array items
    /// Format: PUBSUB subcommand [args...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: PUBSUB subcommand (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("pubsub"));
        }

        // Get subcommand (should be at index 1)
        let subcommand_str = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("Invalid subcommand format"))?
            .to_ascii_uppercase();

        match subcommand_str.as_str() {
            "CHANNELS" => {
                // CHANNELS [pattern]
                let pattern = if items.len() >= 3 {
                    let pattern = items[2]
                        .string_bytes_unchecked()
                        .ok_or(ProtocolError::InvalidArgument("Invalid pattern format"))?
                        .clone();

                    Some(pattern)
                } else {
                    None
                };
                Ok(PubSubParams {
                    subcommand: PubSubSubCommand::Channels(pattern),
                })
            }
            "NUMSUB" => {
                // NUMSUB [channel ...]
                let mut channels = Vec::new();
                for item in items.iter().skip(2) {
                    let channel = item
                        .string_bytes_unchecked()
                        .ok_or(ProtocolError::InvalidArgument("Invalid channel format"))?
                        .clone();
                    channels.push(channel);
                }
                Ok(PubSubParams {
                    subcommand: PubSubSubCommand::NumSub(channels),
                })
            }
            "NUMPAT" => {
                // NUMPAT takes no arguments
                if items.len() > 2 {
                    return Err(ProtocolError::WrongArgCount("pubsub numpat"));
                }
                Ok(PubSubParams {
                    subcommand: PubSubSubCommand::NumPat,
                })
            }
            _ => Err(ProtocolError::UnknownCommand(format!(
                "Unknown PUBSUB subcommand: {}",
                subcommand_str
            ))),
        }
    }
}

impl Display for PubSubParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.subcommand {
            PubSubSubCommand::Channels(pattern) => match pattern {
                Some(p) => write!(f, "PUBSUB CHANNELS {:?}", p),
                None => write!(f, "PUBSUB CHANNELS"),
            },
            PubSubSubCommand::NumSub(channels) => {
                write!(f, "PUBSUB NUMSUB {:?}", channels)
            }
            PubSubSubCommand::NumPat => write!(f, "PUBSUB NUMPAT"),
        }
    }
}

/// PUBSUB command executor
pub struct PubSubCommand;

#[async_trait]
impl Command for PubSubCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = PubSubParams::parse(items)?;
        match params.subcommand {
            PubSubSubCommand::Channels(pattern) => {
                // Get active channels from broadcast layer
                let res = match pattern {
                    None => server.broadcast.pubsub_channels(None).await,
                    Some(v) => server.broadcast.pubsub_channels(Some(&v)).await,
                };
                Ok(res)
            }
            PubSubSubCommand::NumSub(channels) => {
                Ok(server.broadcast.pubsub_numsub(&channels).await)
            }
            PubSubSubCommand::NumPat => Ok(server.broadcast.pubsub_numpat().await),
        }
    }
}
