use crate::error::ProtocolError;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;

#[cfg(all(feature = "lua", feature = "redis"))]
use std::{collections::HashMap, fmt};
#[cfg(all(feature = "lua", feature = "redis"))]
use tracing::warn;

#[cfg(all(feature = "lua", feature = "redis"))]
use crate::protocol::{
    bitmap::{getbit::GetBitCommand, setbit::SetBitCommand},
    hash::{
        hget::HGetCommand, hgetall::HGetAllCommand, hincrby::HIncrByCommand, hkeys::HKeysCommand,
        hmget::HMGetCommand, hset::HSetCommand, hvals::HValsCommand,
    },
    key::{
        del::DelCommand, exists::ExistsCommand, expire::ExpireCommand, persist::PersistCommand,
        pexpire::PExpireCommand, rename::RenameCommand, renamenx::RenameNxCommand,
        type_::TypeCommand,
    },
    list::{
        lindex::LIndexCommand, llen::LLenCommand, lpop::LPopCommand, lpush::LPushCommand,
        lrange::LRangeCommand, lrem::LRemCommand, lset::LSetCommand, rpop::RPopCommand,
        rpush::RPushCommand,
    },
    lua::eval::EvalCommand,
    set::{
        sadd::SAddCommand, sismember::SIsMemberCommand, smembers::SMembersCommand,
        srem::SRemCommand,
    },
    string::{
        append::AppendCommand, get::GetCommand, incr::IncrCommand, incrby::IncrByCommand,
        len::StrLenCommand, mget::MgetCommand, mset::MsetCommand, psetex::PSetExCommand,
        set::SetCommand, setex::SetExCommand, setnx::SetNxCommand,
    },
    zset::{zadd::ZAddCommand, zrange::ZRangeCommand, zrangegetscore::ZRangeByScoreCommand},
};

pub trait RaftCommand: Send + Sync {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError>;
}

pub trait ReadRaftCommand: RaftCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError>;
}

impl<T: ReadRaftCommand> RaftCommand for T {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let operation = self.read_operation(items)?;
        Ok(Operation::Read(operation))
    }
}

/// Command factory for creating and executing commands
///
#[cfg(all(feature = "lua", feature = "redis"))]
pub struct RaftCommandFactory {
    commands: HashMap<String, Box<dyn RaftCommand>>,
}

#[cfg(all(feature = "lua", feature = "redis"))]
impl fmt::Debug for RaftCommandFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftCommandFactory")
            .field("commands", &self.commands.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(all(feature = "lua", feature = "redis"))]
impl RaftCommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: RaftCommand + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init_lua() -> Self {
        let mut factory = Self::new();
        // Register connection commands
        factory.register("GET", GetCommand);
        factory.register("DEL", DelCommand);
        factory.register("INCR", IncrCommand);
        factory.register("INCRBY", IncrByCommand);
        factory.register("MGET", MgetCommand);
        factory.register("LPUSH", LPushCommand);
        factory.register("LRANGE", LRangeCommand);
        factory.register("EXPIRE", ExpireCommand);
        factory.register("PEXPIRE", PExpireCommand);
        factory.register("APPEND", AppendCommand);
        factory.register("HSET", HSetCommand);
        factory.register("HGET", HGetCommand);
        factory.register("HGETALL", HGetAllCommand);
        factory.register("HKEYS", HKeysCommand);
        factory.register("ZADD", ZAddCommand);
        factory.register("ZRANGE", ZRangeCommand);
        factory.register("ZRANGEBYSCORE", ZRangeByScoreCommand);
        factory.register("SADD", SAddCommand);
        factory.register("HINCRBY", HIncrByCommand);
        factory.register("EXISTS", ExistsCommand);
        factory.register("PERSIST", PersistCommand);

        factory.register("SMEMBERS", SMembersCommand);
        factory.register("HMGET", HMGetCommand);
        factory.register("SREM", SRemCommand);
        factory.register("SETBIT", SetBitCommand);
        factory.register("GETBIT", GetBitCommand);
        factory.register("LPOP", LPopCommand);
        factory.register("RPOP", RPopCommand);
        factory.register("STRLEN", StrLenCommand);
        factory.register("HVALS", HValsCommand);
        factory.register("LLEN", LLenCommand);
        factory.register("RPUSH", RPushCommand);
        factory.register("TYPE", TypeCommand);
        factory.register("LINDEX", LIndexCommand);
        factory.register("LREM", LRemCommand);
        factory.register("LSET", LSetCommand);
        factory.register("SISMEMBER", SIsMemberCommand);

        #[cfg(feature = "redis")]
        {
            factory.register("RENAME", RenameCommand);
            factory.register("RENAMENX", RenameNxCommand);
            factory.register("SET", SetCommand);
            factory.register("MSET", MsetCommand);
            factory.register("PSETEX", PSetExCommand);
            factory.register("SETEX", SetExCommand);
            factory.register("SETNX", SetNxCommand);
        }

        #[cfg(all(feature = "lua", feature = "redis"))]
        factory.register("EVAL", EvalCommand); // Prohibiting nesting (not prohibited)

        factory
    }

    pub fn parse_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let cmd_name = match &items[0] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            Value::SimpleString(s) => s.to_uppercase(),
            _ => return Err(ProtocolError::InvalidArgument("command")),
        };
        match self.commands.get(&cmd_name) {
            Some(cmd) => match cmd.raft_request(items) {
                Ok(v) => Ok(v),
                Err(e) => {
                    warn!("Command '{}' error: {}", cmd_name, e);
                    Err(e) // Error → Value::Error
                }
            },
            None => {
                warn!("Unknown command: {}", cmd_name);
                Err(ProtocolError::UnknownCommand(cmd_name))
            }
        }
    }
}
