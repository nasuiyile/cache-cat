use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parsed PING arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingParam {
    pub message: Option<Bytes>,
}

impl fmt::Display for PingParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            None => write!(f, "PING"),
            Some(message) => write!(f, "PING {}", String::from_utf8_lossy(message)),
        }
    }
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
                        Some(s.clone().into())
                    }
                    Value::Integer(i) => {
                        // Integer, convert to string representation
                        Some(i.to_string().into())
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

    /// The reply depends on the arguments alone: PING reads no key, so it
    /// needs neither a cache lookup nor a clock.
    pub fn execute(self) -> Value {
        match self.message {
            None => Value::SimpleString("PONG".to_string()),
            Some(message) => Value::BulkString(Some(message)),
        }
    }
}

impl ReadRaftCommand for PingCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Ping(PingParam::parse(items)?))
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
        let params = PingParam::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Read(ReadOperation::Ping(params)));
            return Ok(Value::queued());
        }
        // Answered by this node without a lease read: a liveness probe must
        // also succeed on a follower and while no leader is known.
        Ok(params.execute())
    }
}
