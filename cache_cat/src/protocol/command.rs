use crate::error::CacheCatError;
use crate::error::ProtocolError;
use crate::protocol::bitmap::getbit::GetBitCommand;
use crate::protocol::bitmap::setbit::SetBitCommand;
use crate::protocol::server::bgsave::BgsaveCommand;
use crate::protocol::connection::echo::EchoCommand;
use crate::protocol::connection::ping::PingCommand;
use crate::protocol::connection::quit::QuitCommand;
use crate::protocol::server::save::SaveCommand;
use crate::protocol::connection::select::SelectCommand;
use crate::protocol::server::time::TimeCommand;
use crate::protocol::hash::hdel::HDelCommand;
use crate::protocol::hash::hget::HGetCommand;
use crate::protocol::hash::hincrby::HIncrByCommand;
use crate::protocol::hash::hmget::{HMGetCommand, HMGetParams};
use crate::protocol::hash::hset::HSetCommand;
use crate::protocol::key::del::DelCommand;
use crate::protocol::key::exists::ExistsCommand;
use crate::protocol::key::expire::ExpireCommand;
use crate::protocol::key::persist::PersistCommand;
use crate::protocol::key::rename::RenameCommand;
use crate::protocol::list::lpush::LPushCommand;
use crate::protocol::list::lrange::LRangeCommand;
use crate::protocol::lua::eval::EvalCommand;
use crate::protocol::lua::evalsha::EvalShaCommand;
use crate::protocol::lua::script::{ScriptCommand, ScriptParam};
use crate::protocol::pub_sub::psubscribe::PsubscribeCommand;
use crate::protocol::pub_sub::publish::PublishCommand;
use crate::protocol::pub_sub::pubsub::PubSubCommand;
use crate::protocol::pub_sub::punsubscribe::PunsubscribeCommand;
use crate::protocol::pub_sub::subscribe::{SubscribeCommand, SubscribeParams};
use crate::protocol::pub_sub::unsubscribe::UnsubscribeCommand;
use crate::protocol::set::sadd::SAddCommand;
use crate::protocol::set::smembers::SMembersCommand;
use crate::protocol::set::srem::SRemCommand;
use crate::protocol::string::append::AppendCommand;
use crate::protocol::string::get::GetCommand;
use crate::protocol::string::incr::IncrCommand;
use crate::protocol::string::incrby::IncrByCommand;
use crate::protocol::string::mget::MgetCommand;
use crate::protocol::string::mset::MsetCommand;
use crate::protocol::string::set::SetCommand;
use crate::protocol::transaction::discard::DiscardCommand;
use crate::protocol::transaction::exec::ExecCommand;
use crate::protocol::transaction::multi::MultiCommand;
use crate::protocol::zset::zadd::ZAddCommand;
use crate::protocol::zset::zrange::ZRangeCommand;
use crate::raft::network::redis_server::{RedisServer, RespCodec};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use futures::StreamExt;
use futures::{Sink, SinkExt, Stream};
use std::collections::HashMap;
use tokio::select;
use tokio::sync::watch;
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

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    pub db_number: u16,
    pub transaction_queue: Option<Vec<Operation>>,
    pub closed: bool,
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
        factory.register("EXISTS", ExistsCommand);
        factory.register("PERSIST", PersistCommand);
        factory.register("RENAME", RenameCommand);
        // List commands
        factory.register("LPUSH", LPushCommand);
        factory.register("LRANGE", LRangeCommand);
        // Hash commands
        factory.register("HSET", HSetCommand);
        factory.register("HGET", HGetCommand);
        factory.register("HINCRBY", HIncrByCommand);
        factory.register("HMGET", HMGetCommand);
        factory.register("HDEL", HDelCommand);
        // Set commands
        factory.register("SADD", SAddCommand);
        factory.register("SMEMBERS", SMembersCommand);
        factory.register("SREM", SRemCommand);
        // ZSet commands
        factory.register("ZADD", ZAddCommand);
        factory.register("ZRANGE", ZRangeCommand);
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
        // Pub/Sub commands
        factory.register("PUBLISH", PublishCommand);
        factory.register_block("SUBSCRIBE", SubscribeCommand);
        factory.register("UNSUBSCRIBE", UnsubscribeCommand);
        factory.register_block("PSUBSCRIBE", PsubscribeCommand);
        factory.register("PUNSUBSCRIBE", PunsubscribeCommand);
        factory.register("PUBSUB", PubSubCommand);

        factory
    }

    /// Parse a RESP value into command name and items
    fn parse_command(value: &Value) -> Result<ParsedCommand, ProtocolError> {
        match value {
            Value::Array(Some(items)) if !items.is_empty() => {
                let name = match &items[0] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => {
                        return Err(ProtocolError::InvalidFormat(
                            "invalid command name".to_string(),
                        ));
                    }
                };
                Ok(ParsedCommand {
                    name,
                    items: items.clone(),
                })
            }
            _ => Err(ProtocolError::InvalidFormat("expected array".to_string())),
        }
    }

    /// Execute a regular command and return its response (without sending)
    async fn execute_regular_command(
        &self,
        cmd_name: &str,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        match self.commands.get(cmd_name) {
            Some(cmd) => cmd.execute(client, items, server).await,
            None => Err(CacheCatError::from(ProtocolError::UnknownCommand(
                cmd_name.to_string(),
            ))),
        }
    }

    /// Handle a command in blocking context (checking if it's allowed)
    async fn handle_command_in_blocking_context<RW>(
        &self,
        parsed: ParsedCommand,
        client: &mut Client,
        server: &RedisServer,
        transport: &mut RW,
        block_cmd: &dyn BlockCommand,
    ) -> Result<(), CacheCatError>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        let resp = block_cmd
            .execute_during_block(client, &parsed, server)
            .await?;
        transport.send(resp).await?;
        Ok(())
    }

    /// Process the blocking command subscription stream
    async fn process_blocking_stream<RW>(
        &self,
        client: &mut Client,
        server: &RedisServer,
        transport: &mut RW,
        block_cmd: &dyn BlockCommand,
        initial_resp: Value,
        mut stream: watch::Receiver<Option<Value>>,
    ) -> Result<(), CacheCatError>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        // Send initial response
        transport.send(initial_resp).await?;

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
                                    transport.send(v).await?;
                                }
                            }
                        }
                        Err(_) => return Ok(()), // Sender dropped
                    }
                }

                // Client sent a new command
                maybe_cmd = transport.next() => {
                    match maybe_cmd {
                        Some(Ok(value)) => {
                            let parsed = match Self::parse_command(&value) {
                                Ok(cmd) => cmd,
                                Err(e) => {
                                    transport.send(Value::from(e)).await?;
                                    continue;
                                }
                            };

                            self.handle_command_in_blocking_context(
                                parsed,
                                client,
                                server,
                                transport,
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

    pub async fn process_connection<RW>(
        &self,
        server: &RedisServer,
        transport: &mut RW,
        mut client: Client,
    ) -> Result<(), CacheCatError>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        loop {
            // Read a command from the transport
            let value = match transport.next().await {
                Some(Ok(v)) => v,
                Some(Err(e)) => {
                    error!("Read error: {}", e);
                    return Err(CacheCatError::from(e));
                }
                None => return Ok(()), // Connection closed
            };

            // Parse and execute the command
            self.execute_command(&mut client, server, transport, value)
                .await?;

            if client.closed {
                return Ok(());
            }
        }
    }

    /// Execute a single command, including handling blocking command subscription streams
    async fn execute_command<RW>(
        &self,
        client: &mut Client,
        server: &RedisServer,
        transport: &mut RW,
        value: Value,
    ) -> Result<(), CacheCatError>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        // Parse the command
        let parsed = match Self::parse_command(&value) {
            Ok(cmd) => cmd,
            Err(e) => {
                transport.send(Value::from(e)).await?;
                return Ok(());
            }
        };

        // Try regular command first
        if self.commands.contains_key(&parsed.name) {
            let resp = match self
                .execute_regular_command(&parsed.name, client, &parsed.items, server)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("Command '{}' error: {}", parsed.name, e);
                    Value::from(e)
                }
            };
            transport.send(resp).await?;
            return Ok(());
        }

        // Try blocking command
        if let Some(cmd) = self.block_commands.get(&parsed.name) {
            match cmd.execute(client, &parsed.items, server).await {
                Ok((initial_resp, stream)) => {
                    return self
                        .process_blocking_stream(
                            client,
                            server,
                            transport,
                            cmd.as_ref(),
                            initial_resp,
                            stream,
                        )
                        .await;
                }
                Err(e) => {
                    transport.send(Value::from(e)).await?;
                    return Ok(());
                }
            }
        }

        // Unknown command
        let resp = Value::from(ProtocolError::UnknownCommand(parsed.name));
        transport.send(resp).await?;
        Ok(())
    }
}
