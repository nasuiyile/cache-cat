//! ECHO command implementation
//!
//! ECHO message
//! Returns the message verbatim.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;

/// ECHO command handler
pub struct EchoCommand;

#[async_trait]
impl Command for EchoCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // ECHO requires exactly one argument
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("echo").into());
        }

        // Return the argument in its original value type (mirrors PING's single‑argument logic)
        let response = match &items[1] {
            Value::BulkString(Some(data)) => Value::BulkString(Some(data.clone())),
            Value::BulkString(None) => Value::BulkString(None),
            Value::SimpleString(s) => Value::SimpleString(s.clone()),
            Value::Integer(i) => Value::Integer(*i),
            Value::Array(_) => return Err(ProtocolError::InvalidArgument("argument type").into()),
            Value::Error(e) => Value::Error(e.clone()),
            _ => return Err(ProtocolError::InvalidArgument("argument type").into()),
        };
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Redis(RedisOperation::RedisReply(response)));
            return Ok(Value::queued());
        }
        Ok(response)
    }
}
