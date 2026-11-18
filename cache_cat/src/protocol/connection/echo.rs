//! ECHO command implementation
//!
//! ECHO message
//! Returns the message verbatim.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parsed ECHO arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoParams {
    /// The argument in the value type the client sent it in.
    pub message: Value,
}

impl fmt::Display for EchoParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ECHO {:?}", self.message)
    }
}

impl EchoParams {
    /// Parse arguments from RESP items
    /// Format: ECHO message
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // ECHO requires exactly one argument
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("echo"));
        }
        // Keep the argument in its original value type (mirrors PING's single-argument logic)
        match &items[1] {
            message @ (Value::BulkString(_)
            | Value::SimpleString(_)
            | Value::Integer(_)
            | Value::Error(_)) => Ok(Self {
                message: message.clone(),
            }),
            _ => Err(ProtocolError::InvalidArgument("argument type")),
        }
    }

    /// The reply is the argument itself: ECHO reads no key, so it needs
    /// neither a cache lookup nor a clock.
    pub fn execute(self) -> Value {
        self.message
    }
}

/// ECHO command handler
pub struct EchoCommand;

impl ReadRaftCommand for EchoCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Echo(EchoParams::parse(items)?))
    }
}

#[async_trait]
impl Command for EchoCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = EchoParams::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Read(ReadOperation::Echo(params)));
            return Ok(Value::queued());
        }
        // Answered by this node without a lease read, like PING.
        Ok(params.execute())
    }
}
