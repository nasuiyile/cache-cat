use crate::network::model::Value;
use crate::protocol::connection::ping::PingCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::set::SetCommand;
use crate::server::handler::rpc::Server;
use async_trait::async_trait;
use std::collections::HashMap;
use crate::protocol::key::del::DelCommand;

#[async_trait]
pub trait Command: Send + Sync {
    /// Execute the command with given RESP items and server context
    async fn execute(&self, items: &[Value], server: &Server) -> Value;
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
    commands: HashMap<String, Box<dyn Command>>,
}

impl CommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init() -> Self {
        let mut factory = Self::new();

        // Register connection commands
        factory.register("GET", GetCommand);
        factory.register("SET", SetCommand);
        factory.register("DEL", DelCommand);
        factory.register("PING", PingCommand);
        factory
    }

    /// Execute a RESP command on the given server
    pub async fn execute(&self, value: Value, server: &Server) -> Value {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                // Extract command name
                let cmd_name = match &items[0] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => return Value::error("invalid command format"),
                };

                // Find and execute command
                match self.commands.get(&cmd_name) {
                    Some(cmd) => cmd.execute(&items, server).await,
                    None => Value::error(format!("unknown command '{}'", cmd_name)),
                }
            }
            _ => Value::error("ERR failed to parse command"),
        }
    }
}
