use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command, SubCommand};
use crate::protocol::connection::client::info::ClientInfoCommand;
use crate::protocol::connection::client::setinfo::SetInfoCommand;
use crate::protocol::connection::client::setname::SetNameCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use std::collections::HashMap;

/// Sentinel command handler
pub struct ClientCommand {
    sub_commands: HashMap<String, Box<dyn SubCommand>>,
}

impl ClientCommand {
    pub fn new() -> Self {
        let mut sub_commands: HashMap<String, Box<dyn SubCommand>> = HashMap::new();
        sub_commands.insert("INFO".to_string(), Box::new(ClientInfoCommand));
        sub_commands.insert("SETNAME".to_string(), Box::new(SetNameCommand));
        sub_commands.insert("SETINFO".to_string(), Box::new(SetInfoCommand));
        Self { sub_commands }
    }
}

impl Default for ClientCommand {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Command for ClientCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("CLIENT").into());
        }

        let sub_command = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            Value::SimpleString(s) => s.to_uppercase(),
            _ => return Err(ProtocolError::InvalidArgument("subcommand").into()),
        };

        match self.sub_commands.get(&sub_command) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(ProtocolError::UnknownCommand(format!("CLIENT {}", sub_command)).into()),
        }
    }
}
