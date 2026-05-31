use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command, SubCommand};
use crate::protocol::sentinel::get_master_addr::SentinelGetMasterAddrByNameCommand;
use crate::protocol::sentinel::masters::SentinelMastersCommand;
use crate::protocol::sentinel::sentinels::SentinelSentinelsCommand;
use crate::protocol::sentinel::slaves::SentinelSlavesCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use std::collections::HashMap;

/// Sentinel command handler
pub struct SentinelCommand {
    sub_commands: HashMap<String, Box<dyn SubCommand>>,
}

impl SentinelCommand {
    pub fn new() -> Self {
        let mut sub_commands: HashMap<String, Box<dyn SubCommand>> = HashMap::new();
        // Register all sentinel sub-commands
        sub_commands.insert("MASTERS".to_string(), Box::new(SentinelMastersCommand));
        sub_commands.insert(
            "GET-MASTER-ADDR-BY-NAME".to_string(),
            Box::new(SentinelGetMasterAddrByNameCommand),
        );
        sub_commands.insert("SLAVES".to_string(), Box::new(SentinelSlavesCommand));
        sub_commands.insert("SENTINELS".to_string(), Box::new(SentinelSentinelsCommand));
        // sub_commands.insert("MASTER".to_string(), Box::new(SentinelMasterCommand));
        // sub_commands.insert("REPLICAS".to_string(), Box::new(SentinelReplicasCommand));
        // sub_commands.insert("RESET".to_string(), Box::new(SentinelResetCommand));
        // sub_commands.insert("FAILOVER".to_string(), Box::new(SentinelFailoverCommand));
        // sub_commands.insert("MONITOR".to_string(), Box::new(SentinelMonitorCommand));
        // sub_commands.insert("REMOVE".to_string(), Box::new(SentinelRemoveCommand));
        // sub_commands.insert("SET".to_string(), Box::new(SentinelSetCommand));
        // sub_commands.insert("INFO-CACHE".to_string(), Box::new(SentinelInfoCacheCommand));
        // sub_commands.insert("PING".to_string(), Box::new(SentinelPingCommand));
        // sub_commands.insert("CKQUORUM".to_string(), Box::new(SentinelCkQuorumCommand));
        // sub_commands.insert("FLUSHCONFIG".to_string(), Box::new(SentinelFlushConfigCommand));

        Self { sub_commands }
    }
}

#[async_trait]
impl Command for SentinelCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("SENTINEL").into());
        }

        let sub_command = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            Value::SimpleString(s) => s.to_uppercase(),
            _ => return Err(ProtocolError::InvalidArgument("subcommand").into()),
        };

        match self.sub_commands.get(&sub_command) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(ProtocolError::UnknownCommand(format!("SENTINEL {}", sub_command)).into()),
        }
    }
}
