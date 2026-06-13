use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// Parsed PING arguments
#[derive(Debug)]
pub struct PingParam {
    pub message: Option<Vec<u8>>,
}

/// PING command handler
pub struct PingCommand;

impl PingParam {
    /// Parse arguments from RESP items
    /// Format: PING [message]
    pub fn parse(items: &[Value]) -> Result<PingParam, ProtocolError> {
        match items.len() {
            1 => {
                // PING (no argument)
                Ok(PingParam { message: None })
            }
            2 => {
                // PING message
                let message = match &items[1] {
                    Value::BulkString(data) => {
                        // BulkString with data or null bulk string
                        data.clone()
                    }
                    Value::SimpleString(s) => {
                        // Simple string, convert to bytes
                        Some(s.as_bytes().to_vec())
                    }
                    Value::Integer(i) => {
                        // Integer, convert to string representation
                        Some(i.to_string().into_bytes())
                    }
                    _ => {
                        return Err(ProtocolError::InvalidArgument(
                            "ping supports only string/integer arguments",
                        ));
                    }
                };
                Ok(PingParam { message })
            }
            _ => {
                // Too many arguments
                Err(ProtocolError::WrongArgCount("ping"))
            }
        }
    }
}

#[async_trait]
impl Command for PingCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse arguments first
        let params = match PingParam::parse(items) {
            Ok(p) => p,
            Err(e) => return Err(e.into()),
        };
        // Check if we're in a transaction
        if let Some(_vec) = client.transaction_queue.as_mut() {}
        // Execute the command
        match params.message {
            None => Ok(Value::SimpleString("PONG".to_string())),
            Some(message) => Ok(Value::BulkString(Some(message))),
        }
    }
}
