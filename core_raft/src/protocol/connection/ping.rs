//! PING command implementation
//!
//! PING [message]
//! Returns PONG if no argument is provided, otherwise returns the message.

use crate::protocol::command::Command;
use async_trait::async_trait;
use crate::network::model::Value;
use crate::server::handler::rpc::Server;

/// PING command handler
pub struct PingCommand;

#[async_trait]
impl Command for PingCommand {
    async fn execute(&self, items: &[Value], _server: &Server) -> Value {
        // PING can have 0 or 1 argument
        // PING -> PONG
        // PING message -> message

        if items.len() > 2 {
            return Value::error("ERR wrong number of arguments for 'ping' command");
        }

        if items.len() == 1 {
            // No argument, return PONG
            Value::SimpleString("PONG".to_string())
        } else {
            // Return the provided message
            match &items[1] {
                Value::BulkString(Some(data)) => Value::BulkString(Some(data.clone())),
                Value::BulkString(None) => Value::BulkString(None),
                Value::SimpleString(s) => Value::SimpleString(s.clone()),
                Value::Integer(i) => Value::Integer(*i),
                Value::Error(e) => Value::Error(e.clone()),
                Value::Array(_) => Value::error("ERR invalid argument type"),
                _ => Value::error("ERR invalid argument type"),
            }
        }
    }
}
