use crate::error::CacheCatError;
use crate::error::ProtocolError;
use crate::protocol::bitmap::getbit::GetBitCommand;
use crate::protocol::bitmap::setbit::SetBitCommand;
use crate::protocol::connection::auth::AuthCommand;
use crate::protocol::connection::client::client::ClientCommand;
use crate::protocol::connection::echo::EchoCommand;
use crate::protocol::connection::hello::HelloCommand;
use crate::protocol::connection::ping::PingCommand;
use crate::protocol::connection::quit::QuitCommand;
use crate::protocol::connection::select::SelectCommand;
use crate::protocol::hash::hdel::HDelCommand;
use crate::protocol::hash::hget::HGetCommand;
use crate::protocol::hash::hgetall::HGetAllCommand;
use crate::protocol::hash::hincrby::HIncrByCommand;
use crate::protocol::hash::hkeys::HKeysCommand;
use crate::protocol::hash::hmget::HMGetCommand;
use crate::protocol::hash::hset::HSetCommand;
use crate::protocol::hash::hvals::HValsCommand;
use crate::protocol::key::del::DelCommand;
use crate::protocol::key::exists::ExistsCommand;
use crate::protocol::key::expire::ExpireCommand;
use crate::protocol::key::persist::PersistCommand;
use crate::protocol::key::pexpire::PExpireCommand;
use crate::protocol::key::rename::RenameCommand;
use crate::protocol::key::renamenx::RenameNxCommand;
use crate::protocol::list::llen::LLenCommand;
use crate::protocol::list::lpush::LPushCommand;
use crate::protocol::list::lrange::LRangeCommand;
use crate::protocol::list::rpush::RPushCommand;
use crate::protocol::lua::eval::EvalCommand;
use crate::protocol::lua::evalsha::EvalShaCommand;
use crate::protocol::lua::script::ScriptCommand;
use crate::protocol::pub_sub::psubscribe::PsubscribeCommand;
use crate::protocol::pub_sub::publish::PublishCommand;
use crate::protocol::pub_sub::pubsub::PubSubCommand;
use crate::protocol::pub_sub::punsubscribe::PunsubscribeCommand;
use crate::protocol::pub_sub::subscribe::SubscribeCommand;
use crate::protocol::pub_sub::unsubscribe::UnsubscribeCommand;
use crate::protocol::sentinel::sentinel::SentinelCommand;
use crate::protocol::server::bgsave::BgsaveCommand;
use crate::protocol::server::save::SaveCommand;
use crate::protocol::server::shutdown::ShutdownCommand;
use crate::protocol::server::time::TimeCommand;
use crate::protocol::set::sadd::SAddCommand;
use crate::protocol::set::smembers::SMembersCommand;
use crate::protocol::set::srem::SRemCommand;
use crate::protocol::string::append::AppendCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::incr::IncrCommand;
use crate::protocol::string::incrby::IncrByCommand;
use crate::protocol::string::len::StrLenCommand;
use crate::protocol::string::mget::MgetCommand;
use crate::protocol::string::mset::MsetCommand;
use crate::protocol::string::psetex::PSetExCommand;
use crate::protocol::string::set::SetCommand;
use crate::protocol::string::setex::SetExCommand;
use crate::protocol::string::setnx::SetNxCommand;
use crate::protocol::transaction::discard::DiscardCommand;
use crate::protocol::transaction::exec::ExecCommand;
use crate::protocol::transaction::multi::MultiCommand;
use crate::protocol::zset::zadd::ZAddCommand;
use crate::protocol::zset::zrange::ZRangeCommand;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreCommand;
use crate::raft::network::redis_server::{RedisServer, RespCodec};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::utils::now_ms;
use async_trait::async_trait;
use futures::SinkExt;
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::watch;
use tokio_util::codec::Framed;
use tracing::{error, warn};

#[async_trait]
pub trait Command: Send + Sync {
    /// Execute the command with given RESP items and server context
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError>;
}

#[async_trait]
pub trait BlockCommand: Send + Sync {
    /// Execute the command with given RESP items and server context
    /// Returns initial response and a watch receiver for subsequent messages
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<(Value, watch::Receiver<Option<Value>>), CacheCatError>;
    async fn execute_during_block(
        &self,
        client: &mut Client,
        cmd: &ParsedCommand,
        server: &RedisServer,
    ) -> Result<Value, CacheCatError>;
}

/// Command trait for sub-command registration
#[async_trait]
pub trait SubCommand: Send + Sync {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError>;
}

pub struct Client {
    pub id: u64,
    pub db_number: u16,
    pub transaction_queue: Option<Vec<Operation>>,
    pub closed: bool,
    pub authenticated: bool,
    pub framed: Framed<TcpStream, RespCodec>,
    pub name: String,
    pub connection_time: u64,
    pub last_interaction: u64,
    pub flag: ClientFlag,
    pub last_cmd: String,
    pub lib_name: String,
    pub lib_ver: String,
}

impl Client {
    pub fn new(id: u64, framed: Framed<TcpStream, RespCodec>, auth: bool) -> Self {
        Self {
            id,
            db_number: 0,
            transaction_queue: None,
            closed: false,
            authenticated: auth,
            framed,
            name: "".to_string(),
            connection_time: now_ms(),
            last_interaction: now_ms(),
            flag: ClientFlag::new(),
            last_cmd: "".to_string(),
            lib_name: "".to_string(),
            lib_ver: "".to_string(),
        }
    }
}
pub struct ClientFlag {
    pub in_sub: bool,
    pub multi: bool,
    pub blocking: bool,
}

impl ClientFlag {
    #[inline]
    pub const fn new() -> Self {
        Self {
            in_sub: false,
            multi: false,
            blocking: false,
        }
    }
}

impl Default for ClientFlag {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Display for ClientFlag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut flags = String::new();

        if self.in_sub {
            flags.push('P');
        }
        if self.multi {
            flags.push('x');
        }
        if self.blocking {
            flags.push('b');
        }
        // Redis 的规则：没有其它 flag 时显示 N
        if flags.is_empty() {
            flags.push('N');
        }
        write!(f, "{flags}")
    }
}

/// Parsed command information
pub struct ParsedCommand {
    pub name: String,
    pub items: Vec<Value>,
}

/// Command factory for creating and executing commands
pub struct CommandFactory {
    commands: HashMap<String, Box<dyn Command>>,
    block_commands: HashMap<String, Box<dyn BlockCommand>>,
}

impl CommandFactory {
    /// Create a new empty command factory
    fn new() -> Self {
        Self {
            commands: HashMap::new(),
            block_commands: HashMap::new(),
        }
    }

    /// Register a command with given name
    fn register<C: Command + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.commands.insert(name.into(), Box::new(cmd));
    }

    fn register_block<C: BlockCommand + 'static>(&mut self, name: impl Into<String>, cmd: C) {
        self.block_commands.insert(name.into(), Box::new(cmd));
    }

    /// Initialize the command factory with all supported commands
    pub fn init() -> Self {
        let mut factory = Self::new();
        // Register connection commands
        factory.register("PING", PingCommand);
        factory.register("ECHO", EchoCommand);
        factory.register("TIME", TimeCommand);
        factory.register("SELECT", SelectCommand);
        factory.register("QUIT", QuitCommand);
        factory.register("AUTH", AuthCommand);
        factory.register("CLIENT", ClientCommand::new());
        factory.register("HELLO", HelloCommand);
        // Register data commands
        factory.register("GET", GetCommand);
        factory.register("SET", SetCommand);
        factory.register("DEL", DelCommand);
        factory.register("INCR", IncrCommand);
        factory.register("INCRBY", IncrByCommand);
        factory.register("MSET", MsetCommand);
        factory.register("MGET", MgetCommand);
        factory.register("APPEND", AppendCommand);
        factory.register("EXPIRE", ExpireCommand);
        factory.register("PEXPIRE", PExpireCommand);
        factory.register("EXISTS", ExistsCommand);
        factory.register("PERSIST", PersistCommand);
        factory.register("RENAME", RenameCommand);
        factory.register("RENAMENX", RenameNxCommand);
        factory.register("PSETEX", PSetExCommand);
        factory.register("SETEX", SetExCommand);
        factory.register("SETNX", SetNxCommand);
        factory.register("STRLEN", StrLenCommand);
        // List commands
        factory.register("LPUSH", LPushCommand);
        factory.register("RPUSH", RPushCommand);
        factory.register("LRANGE", LRangeCommand);
        factory.register("LLEN", LLenCommand);
        // Hash commands
        factory.register("HSET", HSetCommand);
        factory.register("HGET", HGetCommand);
        factory.register("HINCRBY", HIncrByCommand);
        factory.register("HMGET", HMGetCommand);
        factory.register("HDEL", HDelCommand);
        factory.register("HGETALL", HGetAllCommand);
        factory.register("HKEYS", HKeysCommand);
        factory.register("HVALS", HValsCommand);

        // Set commands
        factory.register("SADD", SAddCommand);
        factory.register("SMEMBERS", SMembersCommand);
        factory.register("SREM", SRemCommand);
        // ZSet commands
        factory.register("ZADD", ZAddCommand);
        factory.register("ZRANGE", ZRangeCommand);
        factory.register("ZRANGEBYSCORE", ZRangeByScoreCommand);
        // Bitmap commands
        factory.register("SETBIT", SetBitCommand);
        factory.register("GETBIT", GetBitCommand);
        // Lua scripting
        factory.register("EVAL", EvalCommand);
        factory.register("EVALSHA", EvalShaCommand);
        factory.register("SCRIPT", ScriptCommand);
        // Transaction commands
        factory.register("MULTI", MultiCommand);
        factory.register("DISCARD", DiscardCommand);
        factory.register("EXEC", ExecCommand);
        // Connection management
        factory.register("BGSAVE", BgsaveCommand);
        factory.register("SAVE", SaveCommand);
        factory.register("SHUTDOWN", ShutdownCommand);
        // Pub/Sub commands
        factory.register("PUBLISH", PublishCommand);
        factory.register_block("SUBSCRIBE", SubscribeCommand);
        factory.register("UNSUBSCRIBE", UnsubscribeCommand);
        factory.register_block("PSUBSCRIBE", PsubscribeCommand);
        factory.register("PUNSUBSCRIBE", PunsubscribeCommand);
        factory.register("PUBSUB", PubSubCommand);
        //Sentinel
        factory.register("SENTINEL", SentinelCommand::new());

        factory
    }

    /// Parse a RESP value into command name and items
    fn parse_command(value: &Value) -> Result<ParsedCommand, ProtocolError> {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                let name = items[0]
                    .as_str_lossy()
                    .ok_or(ProtocolError::InvalidFormat(
                        "invalid command name".to_string(),
                    ))?
                    .to_string();

                Ok(ParsedCommand {
                    name,
                    items: items.clone(),
                })
            }
            _ => Err(ProtocolError::InvalidFormat("expected array".to_string())),
        }
    }

    /// Handle a command in blocking context (checking if it's allowed)
    async fn handle_command_in_blocking_context(
        &self,
        parsed: ParsedCommand,
        client: &mut Client,
        server: &RedisServer,
        block_cmd: &dyn BlockCommand,
    ) -> Result<(), CacheCatError> {
        let resp = block_cmd
            .execute_during_block(client, &parsed, server)
            .await?;
        client.framed.send(resp).await?;
        Ok(())
    }

    /// Process the blocking command subscription stream
    async fn process_blocking_stream(
        &self,
        client: &mut Client,
        server: &RedisServer,
        block_cmd: &dyn BlockCommand,
        initial_resp: Value,
        mut stream: watch::Receiver<Option<Value>>,
    ) -> Result<(), CacheCatError> {
        // Send initial response
        client.framed.send(initial_resp).await?;
        // Enter blocking mode: listen to both subscription stream and new commands
        loop {
            select! {
                // Subscription stream has new data
                change = stream.changed() => {
                    match change {
                        Ok(_) => {
                            let val = stream.borrow().clone();
                            match val {
                                None => return Ok(()), // Subscription ended
                                Some(v) => {
                                    client.framed.send(v).await?;
                                }
                            }
                        }
                        Err(_) => return Ok(()), // Sender dropped
                    }
                }

                // Client sent a new command
                maybe_cmd = client.framed.next() => {
                    match maybe_cmd {
                        Some(Ok(value)) => {
                            let parsed = match Self::parse_command(&value) {
                                Ok(cmd) => cmd,
                                Err(e) => {
                                    client.framed.send(Value::from(e)).await?;
                                    continue;
                                }
                            };

                            self.handle_command_in_blocking_context(
                                parsed,
                                client,
                                server,
                                block_cmd,
                            ).await?;
                            if client.closed{
                                return Ok(());
                            }
                        }
                        Some(Err(e)) => {
                            error!("Read error in blocking mode: {}", e);
                            return Err(CacheCatError::from(e));
                        }
                        None => return Ok(()), // Connection closed
                    }
                }
            }
        }
    }

    pub async fn process_connection(
        &self,
        server: &RedisServer,
        mut client: Client,
    ) -> Result<(), CacheCatError> {
        loop {
            // Read a command from the transport
            let value = match client.framed.next().await {
                Some(Ok(v)) => v,
                Some(Err(e)) => {
                    error!("Read error: {}", e);
                    return Err(CacheCatError::from(e));
                }
                None => return Ok(()), // Connection closed
            };
            // Parse and execute the command
            self.execute_command(&mut client, server, value).await?;
            if client.closed {
                return Ok(());
            }
        }
    }

    /// Execute a single command, including handling blocking command subscription streams
    async fn execute_command(
        &self,
        client: &mut Client,
        server: &RedisServer,
        value: Value,
    ) -> Result<(), CacheCatError> {
        client.last_interaction = now_ms();
        // Parse the command
        let parsed = match Self::parse_command(&value) {
            Ok(cmd) => cmd,
            Err(e) => {
                client.framed.send(Value::from(e)).await?;
                return Ok(());
            }
        };
        client.last_cmd = parsed.name.clone();
        if !client.authenticated && parsed.name != "AUTH" && parsed.name != "QUIT" {
            client
                .framed
                .send(Value::from(ProtocolError::NotAuthenticated))
                .await?;
            return Ok(());
        }

        if let Some(cmd) = self.commands.get(&parsed.name) {
            let resp = match cmd.execute(client, &parsed.items, server).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Command '{}' error: {}", parsed.name, e);
                    Value::from(e)
                }
            };
            client.framed.send(resp).await?;
            return Ok(());
        }

        // Try blocking command
        if let Some(cmd) = self.block_commands.get(&parsed.name) {
            match cmd.execute(client, &parsed.items, server).await {
                Ok((initial_resp, stream)) => {
                    client.flag.blocking = true;
                    let result = self
                        .process_blocking_stream(client, server, cmd.as_ref(), initial_resp, stream)
                        .await;
                    client.flag.blocking = false;
                    client.flag.in_sub = false;
                    return result;
                }
                Err(e) => {
                    client.framed.send(Value::from(e)).await?;
                    return Ok(());
                }
            }
        }

        // Unknown command
        let resp = Value::from(ProtocolError::UnknownCommand(parsed.name));
        client.framed.send(resp).await?;
        Ok(())
    }
}
