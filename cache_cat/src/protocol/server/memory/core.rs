use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use std::collections::HashMap;
use crate::protocol::server::memory::doctor::MemoryDoctorCommand;
use crate::protocol::server::memory::malloc_stats::MemoryMallocStatsCommand;
use crate::protocol::server::memory::purge::MemoryPurgeCommand;
use crate::protocol::server::memory::stats::MemoryStatsCommand;
use crate::protocol::server::memory::usage::MemoryUsageCommand;

/// MEMORY command handler
pub struct MemoryCommand {
    sub_commands: HashMap<String, Box<dyn SubCommand>>,
}

impl MemoryCommand {
    pub fn new() -> Self {
        let mut sub_commands: HashMap<String, Box<dyn SubCommand>> = HashMap::new();
        sub_commands.insert("USAGE".to_string(), Box::new(MemoryUsageCommand));
        sub_commands.insert("STATS".to_string(), Box::new(MemoryStatsCommand));
        sub_commands.insert("PURGE".to_string(), Box::new(MemoryPurgeCommand));
        sub_commands.insert("MALLOC-STATS".to_string(), Box::new(MemoryMallocStatsCommand));
        sub_commands.insert("DOCTOR".to_string(), Box::new(MemoryDoctorCommand));
        Self { sub_commands }
    }
}

impl Default for MemoryCommand {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Command for MemoryCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("MEMORY").into());
        }

        let sub_command = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            Value::SimpleString(s) => s.to_uppercase(),
            _ => return Err(ProtocolError::InvalidArgument("subcommand").into()),
        };
        println!("{}", sub_command);

        match self.sub_commands.get(&sub_command) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(ProtocolError::UnknownCommand(format!("MEMORY {}", sub_command)).into()),
        }
    }
}
