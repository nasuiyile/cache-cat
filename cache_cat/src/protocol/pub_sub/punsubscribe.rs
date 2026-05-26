//! PUNSUBSCRIBE command implementation
//!
//! PUNSUBSCRIBE [pattern [pattern ...]]
//! Unsubscribes the client from the given patterns, or from all of
//! them if none is given.
//!
//! When no patterns are specified, the client is unsubscribed from all
//! the previously subscribed patterns. In this case, a message for every
//! unsubscribed pattern will be sent to the client.
//!
//! Returns:
//! - For each pattern unsubscribed: a multi-bulk reply with three elements:
//!   - "punsubscribe"
//!   - pattern
//!   - number of patterns the client is currently subscribed to

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// PUNSUBSCRIBE command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PunsubscribeParams {
    /// Patterns to unsubscribe from.
    /// None means unsubscribe from all patterns.
    pub patterns: Option<Vec<Vec<u8>>>,
}

impl PunsubscribeParams {
    /// Parse PUNSUBSCRIBE command parameters from RESP array items
    /// Format: PUNSUBSCRIBE [pattern [pattern ...]]
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        match items.len() {
            // PUNSUBSCRIBE with no arguments - unsubscribe from all patterns
            1 => Ok(PunsubscribeParams { patterns: None }),
            // PUNSUBSCRIBE pattern [pattern ...]
            _ => {
                let mut patterns = Vec::with_capacity(items.len() - 1);

                for item in &items[1..] {
                    let pattern = match item {
                        Value::BulkString(Some(data)) => data.clone(),
                        Value::SimpleString(s) => s.as_bytes().to_vec(),
                        _ => return Err(ProtocolError::WrongArgCount("punsubscribe")),
                    };
                    patterns.push(pattern);
                }

                Ok(PunsubscribeParams {
                    patterns: Some(patterns),
                })
            }
        }
    }
}

impl Display for PunsubscribeParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.patterns {
            Some(patterns) => write!(f, "PunsubscribeReq {{ patterns: {:?} }}", patterns),
            None => write!(f, "PunsubscribeReq {{ patterns: all }}"),
        }
    }
}

/// PUNSUBSCRIBE command executor
pub struct PunsubscribeCommand;

#[async_trait]
impl Command for PunsubscribeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = PunsubscribeParams::parse(items)?;
        let result = match params.patterns {
            None => server.broadcast.punsubscribe_all_patterns(client.id).await,
            Some(patterns) => server.broadcast.punsubscribe(patterns, client.id).await,
        };
        Ok(result)
    }
}
