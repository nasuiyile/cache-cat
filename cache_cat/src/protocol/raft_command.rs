use crate::error::ProtocolError;
use crate::protocol::bitmap::getbit::GetBitCommand;
use crate::protocol::bitmap::setbit::SetBitCommand;
use crate::protocol::hash::hexists::HExistsCommand;
use crate::protocol::hash::hget::HGetCommand;
use crate::protocol::hash::hgetall::HGetAllCommand;
use crate::protocol::hash::hincrby::HIncrByCommand;
use crate::protocol::hash::hkeys::HKeysCommand;
use crate::protocol::hash::hlen::HLenCommand;
use crate::protocol::hash::hmget::HMGetCommand;
use crate::protocol::hash::hset::HSetCommand;
use crate::protocol::hash::hsetnx::HSetNxCommand;
use crate::protocol::hash::hvals::HValsCommand;
use crate::protocol::key::del::DelCommand;
use crate::protocol::key::exists::ExistsCommand;
use crate::protocol::key::expire::ExpireCommand;
use crate::protocol::key::persist::PersistCommand;
use crate::protocol::key::pexpire::PExpireCommand;
use crate::protocol::key::pttl::PTtlCommand;
use crate::protocol::key::rename::RenameCommand;
use crate::protocol::key::renamenx::RenameNxCommand;
use crate::protocol::key::ttl::TtlCommand;
use crate::protocol::key::type_::TypeCommand;
use crate::protocol::list::lindex::LIndexCommand;
use crate::protocol::list::llen::LLenCommand;
use crate::protocol::list::lpop::LPopCommand;
use crate::protocol::list::lpush::LPushCommand;
use crate::protocol::list::lrange::LRangeCommand;
use crate::protocol::list::lrem::LRemCommand;
use crate::protocol::list::lset::LSetCommand;
use crate::protocol::list::ltrim::LTrimCommand;
use crate::protocol::list::rpop::RPopCommand;
use crate::protocol::list::rpush::RPushCommand;
use crate::protocol::lua::eval::EvalCommand;
use crate::protocol::set::sadd::SAddCommand;
use crate::protocol::set::sismember::SIsMemberCommand;
use crate::protocol::set::smembers::SMembersCommand;
use crate::protocol::set::srem::SRemCommand;
use crate::protocol::string::append::AppendCommand;
use crate::protocol::string::decr::DecrCommand;
use crate::protocol::string::decrby::DecrByCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::getset::GetSetCommand;
use crate::protocol::string::incr::IncrCommand;
use crate::protocol::string::incrby::IncrByCommand;
use crate::protocol::string::len::StrLenCommand;
use crate::protocol::string::mget::MgetCommand;
use crate::protocol::string::mset::MsetCommand;
use crate::protocol::string::psetex::PSetExCommand;
use crate::protocol::string::set::SetCommand;
use crate::protocol::string::setex::SetExCommand;
use crate::protocol::string::setnx::SetNxCommand;
use crate::protocol::zset::zadd::ZAddCommand;
use crate::protocol::zset::zrange::ZRangeCommand;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreCommand;
use crate::protocol::zset::zrem::ZRemCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use std::collections::HashMap;
use std::fmt;
use tracing::warn;
use crate::protocol::bitmap::bitcount::BitCountCommand;
use crate::protocol::key::flushdb::FlushDBCommand;

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
pub struct RaftCommandFactory {
    commands: HashMap<String, Box<dyn RaftCommand>>,
}
impl fmt::Debug for RaftCommandFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftCommandFactory")
            .field("commands", &self.commands.keys().collect::<Vec<_>>())
            .finish()
    }
}

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
        factory.register("SET", SetCommand);
        factory.register("DEL", DelCommand);
        factory.register("INCR", IncrCommand);
        factory.register("INCRBY", IncrByCommand);
        factory.register("MSET", MsetCommand);
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
        factory.register("RENAME", RenameCommand);
        factory.register("RENAMENX", RenameNxCommand);
        factory.register("SMEMBERS", SMembersCommand);
        factory.register("HMGET", HMGetCommand);
        factory.register("EVAL", EvalCommand); // Prohibiting nesting (not prohibited)
        factory.register("SREM", SRemCommand);
        factory.register("SETBIT", SetBitCommand);
        factory.register("GETBIT", GetBitCommand);
        factory.register("LPOP", LPopCommand);
        factory.register("RPOP", RPopCommand);
        factory.register("PSETEX", PSetExCommand);
        factory.register("SETEX", SetExCommand);
        factory.register("SETNX", SetNxCommand);
        factory.register("STRLEN", StrLenCommand);
        factory.register("HVALS", HValsCommand);
        factory.register("LLEN", LLenCommand);
        factory.register("RPUSH", RPushCommand);
        factory.register("TYPE", TypeCommand);
        factory.register("LINDEX", LIndexCommand);
        factory.register("LREM", LRemCommand);
        factory.register("LSET", LSetCommand);
        factory.register("SISMEMBER", SIsMemberCommand);
        factory.register("HEXISTS", HExistsCommand);
        factory.register("DECRBY", DecrByCommand);
        factory.register("PTTL", PTtlCommand);
        factory.register("TTL", TtlCommand);
        factory.register("HLEN", HLenCommand);
        factory.register("HSETNX", HSetNxCommand);
        factory.register("DECR", DecrCommand);
        factory.register("GETSET", GetSetCommand);
        factory.register("ZREM", ZRemCommand);
        factory.register("LTRIM", LTrimCommand);
        factory.register("FLUSHDB", FlushDBCommand);
        factory.register("BITCOUNT", BitCountCommand);
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
