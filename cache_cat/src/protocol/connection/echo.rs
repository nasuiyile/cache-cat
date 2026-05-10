//! ECHO command implementation
//!
//! ECHO message
//! Returns the message verbatim.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// ECHO command handler
pub struct EchoCommand;

#[async_trait]
impl Command for EchoCommand {
    async fn execute(
        &self,
        _db_number: &mut u16,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // ECHO requires exactly one argument
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("echo").into());
        }

        // Return the argument in its original value type (mirrors PING's single‑argument logic)
        match &items[1] {
            Value::BulkString(Some(data)) => Ok(Value::BulkString(Some(data.clone()))),
            Value::BulkString(None) => Ok(Value::BulkString(None)),
            Value::SimpleString(s) => Ok(Value::SimpleString(s.clone())),
            Value::Integer(i) => Ok(Value::Integer(*i)),
            Value::Array(_) => Err(ProtocolError::InvalidArgument("argument type").into()),
            Value::Error(e) => Ok(Value::Error(e.clone())),
            _ => Err(ProtocolError::InvalidArgument("argument type").into()),
        }
    }
}
