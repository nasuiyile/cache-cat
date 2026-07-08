use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command, SubCommand};
use crate::protocol::connection::client::info::ClientInfoCommand;
use crate::protocol::connection::client::setinfo::SetInfoCommand;
use crate::protocol::connection::client::setname::SetNameCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use rustc_hash::FxHashMap;

/// Sentinel command handler
#[repr(transparent)]
pub struct ClientCommand {
    sub_commands: FxHashMap<&'static str, Box<dyn SubCommand>>,
}

// TODO: can use LazyLock to replace the field
impl ClientCommand {
    pub fn new() -> Self {
        let mut sub_commands: FxHashMap<&'static str, Box<dyn SubCommand>> = FxHashMap::default();
        sub_commands.insert("INFO", Box::new(ClientInfoCommand));
        sub_commands.insert("SETNAME", Box::new(SetNameCommand));
        sub_commands.insert("SETINFO", Box::new(SetInfoCommand));
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

        let sub_command = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?
            .to_uppercase();

        match self.sub_commands.get(sub_command.as_str()) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(ProtocolError::UnknownCommand(format!("CLIENT {}", sub_command)).into()),
        }
    }
}
