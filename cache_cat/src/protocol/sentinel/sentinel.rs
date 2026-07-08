use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command, SubCommand};
use crate::protocol::sentinel::get_master_addr::SentinelGetMasterAddrByNameCommand;
use crate::protocol::sentinel::masters::SentinelMastersCommand;
use crate::protocol::sentinel::sentinels::SentinelSentinelsCommand;
use crate::protocol::sentinel::slaves::SentinelSlavesCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use rustc_hash::FxHashMap;

/// Sentinel command handler
#[repr(transparent)]
pub struct SentinelCommand {
    sub_commands: FxHashMap<&'static str, Box<dyn SubCommand>>,
}

// TODO: can use LazyLock to replace the field
impl SentinelCommand {
    pub fn new() -> Self {
        let mut sub_commands: FxHashMap<&'static str, Box<dyn SubCommand>> = FxHashMap::default();
        // Register all sentinel sub-commands
        sub_commands.insert("MASTERS", Box::new(SentinelMastersCommand));
        sub_commands.insert(
            "GET-MASTER-ADDR-BY-NAME",
            Box::new(SentinelGetMasterAddrByNameCommand),
        );
        sub_commands.insert("SLAVES", Box::new(SentinelSlavesCommand));
        sub_commands.insert("SENTINELS", Box::new(SentinelSentinelsCommand));
        // sub_commands.insert("MASTER", Box::new(SentinelMasterCommand));
        // sub_commands.insert("REPLICAS", Box::new(SentinelReplicasCommand));
        // sub_commands.insert("RESET", Box::new(SentinelResetCommand));
        // sub_commands.insert("FAILOVER", Box::new(SentinelFailoverCommand));
        // sub_commands.insert("MONITOR", Box::new(SentinelMonitorCommand));
        // sub_commands.insert("REMOVE", Box::new(SentinelRemoveCommand));
        // sub_commands.insert("SET", Box::new(SentinelSetCommand));
        // sub_commands.insert("INFO-CACHE", Box::new(SentinelInfoCacheCommand));
        // sub_commands.insert("PING", Box::new(SentinelPingCommand));
        // sub_commands.insert("CKQUORUM", Box::new(SentinelCkQuorumCommand));
        // sub_commands.insert("FLUSHCONFIG", Box::new(SentinelFlushConfigCommand));

        Self { sub_commands }
    }
}

impl Default for SentinelCommand {
    #[inline]
    fn default() -> Self {
        Self::new()
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

        let sub_command = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?
            .to_uppercase();

        match self.sub_commands.get(sub_command.as_str()) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(ProtocolError::UnknownCommand(format!("SENTINEL {}", sub_command)).into()),
        }
    }
}
