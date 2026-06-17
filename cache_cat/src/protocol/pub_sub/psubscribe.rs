//! PSUBSCRIBE command implementation
//!
//! PSUBSCRIBE pattern [pattern ...]
//! Subscribes the client to the specified patterns.
//!
//! Once the client enters the subscribed state, it is not supposed to
//! issue any other commands, except for additional SUBSCRIBE, PSUBSCRIBE,
//! UNSUBSCRIBE, PUNSUBSCRIBE, PING, RESET and QUIT commands.
//!
//! Returns:
//! - For each pattern subscribed: a multi-bulk reply with three elements:
//!   - "psubscribe"
//!   - pattern
//!   - number of patterns the client is currently subscribed to

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{BlockCommand, Client, ParsedCommand};
use crate::protocol::connection::ping::PingParam;
use crate::protocol::pub_sub::punsubscribe::PunsubscribeParams;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tokio::sync::watch;

/// PSUBSCRIBE command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PsubscribeParams {
    pub patterns: Vec<Bytes>,
}

impl PsubscribeParams {
    /// Parse PSUBSCRIBE command parameters from RESP array items
    /// Format: PSUBSCRIBE pattern [pattern ...]
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: PSUBSCRIBE pattern (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("psubscribe"));
        }

        let patterns = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if patterns.len() < items.len() - 1 {
            return Err(ProtocolError::WrongArgCount("psubscribe"));
        }

        Ok(PsubscribeParams { patterns })
    }
}

impl Display for PsubscribeParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PsubscribeReq {{ patterns: {:?} }}", self.patterns)
    }
}

/// PSUBSCRIBE command executor
pub struct PsubscribeCommand;

#[async_trait]
impl BlockCommand for PsubscribeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<(Value, watch::Receiver<Option<Value>>), CacheCatError> {
        let params = PsubscribeParams::parse(items)?;
        client.flag.in_sub = true;
        Ok(server
            .broadcast
            .psubscribe(params.patterns, client.id)
            .await)
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
