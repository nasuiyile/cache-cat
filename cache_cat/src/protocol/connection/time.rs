//! TIME command implementation
//!
//! TIME
//! Returns the current server time as a two-element array:
//! - Unix timestamp in seconds
//! - Microseconds

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::utils::now_ms;
use crate::utils::times::{now_s, now_us};
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};

/// TIME command handler
pub struct TimeCommand;
#[async_trait]
impl Command for TimeCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // TIME takes no arguments
        if items.len() != 1 {
            return Err(ProtocolError::WrongArgCount("time").into());
        }

        // Get current system time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| ProtocolError::Custom("Failed to get system time"))?;

        let seconds = now.as_secs();
        let microseconds = now.subsec_micros();

        // Return as array: [seconds, microseconds]
        Ok(Value::Array(Some(vec![
            Value::BulkString(Some(seconds.to_string().into_bytes())),
            Value::BulkString(Some(microseconds.to_string().into_bytes())),
        ])))
    }
}
