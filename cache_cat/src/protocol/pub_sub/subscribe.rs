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
use crate::protocol::command::{BlockCommand, Client, ParsedCommand};
use crate::protocol::connection::ping::PingParam;
use crate::protocol::pub_sub::psubscribe::PsubscribeParams;
use crate::protocol::pub_sub::punsubscribe::PunsubscribeParams;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::sync::watch;

/// SUBSCRIBE command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeParams {
    pub channels: Vec<Bytes>,
}

impl SubscribeParams {
    /// Parse SUBSCRIBE command parameters from RESP array items
    /// Format: SUBSCRIBE channel [channel ...]
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: SUBSCRIBE channel (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("subscribe"));
        }

        let channels = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if channels.len() < items.len() - 1 {
            return Err(ProtocolError::WrongArgCount("subscribe"));
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
        client.flag.in_sub = true;
        Ok(server.broadcast.subscribe(params.channels, client.id).await)
    }

    async fn execute_during_block(
        &self,
        client: &mut Client,
        cmd: &ParsedCommand,
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if cmd.name == "PUNSUBSCRIBE" {
            let params = PunsubscribeParams::parse(&cmd.items)?;
            let result = match params.patterns {
                None => server.broadcast.punsubscribe_all_patterns(client.id).await,
                Some(patterns) => server.broadcast.punsubscribe(patterns, client.id).await,
            };
            Ok(result)
        } else if cmd.name == "PSUBSCRIBE" {
            let params = PsubscribeParams::parse(&cmd.items)?;
            Ok(server
                .broadcast
                .psubscribe(params.patterns, client.id)
                .await
                .0)
        } else if cmd.name == "SUBSCRIBE" {
            let params = crate::protocol::pub_sub::subscribe::SubscribeParams::parse(&cmd.items)?;
            Ok(server
                .broadcast
                .subscribe(params.channels, client.id)
                .await
                .0)
        } else if cmd.name == "UNSUBSCRIBE" {
            let params =
                crate::protocol::pub_sub::unsubscribe::UnsubscribeParams::parse(&cmd.items)?;
            let result = match params.channels {
                None => server.broadcast.unsubscribe_all_channels(client.id).await,
                Some(channels) => server.broadcast.unsubscribe(channels, client.id).await,
            };
            Ok(result)
        } else if cmd.name == "PING" {
            let params = PingParam::parse(&cmd.items)?;
            return Ok(Value::Array(Some(vec![
                Value::from_static_string("PONG"),
                Value::BulkString(params.message),
            ])));
        } else if cmd.name == "QUIT" {
            client.closed = true;
            return Ok(Value::ok());
        } else {
            let resp = Value::error(
                "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context",
            );
            return Ok(resp);
        }
    }
}
