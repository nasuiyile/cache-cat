use crate::error::CacheCatError;
use crate::protocol::bitmap::getbit::GetBitCommand;
use crate::protocol::bitmap::setbit::SetBitCommand;
use crate::protocol::connection::bgsave::BgsaveCommand;
use crate::protocol::connection::echo::EchoCommand;
use crate::protocol::connection::ping::PingCommand;
use crate::protocol::connection::save::SaveCommand;
use crate::protocol::connection::select::SelectCommand;
use crate::protocol::connection::time::TimeCommand;
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
use crate::protocol::pub_sub::publish::PublishCommand;
use crate::protocol::pub_sub::subscribe::SubscribeCommand;
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
use futures::stream::SplitSink;
use futures::{Sink, SinkExt, Stream};
use std::collections::HashMap;
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

    /// Check if this command can handle the given unblock command
    /// Returns true if this command can handle the unblock request
    fn can_handle_unblock(&self, cmd_name: &str) -> bool;
}

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    pub db_number: u16,
    pub transaction_queue: Option<Vec<Operation>>,
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

        // Block commands (can be unblocked)
        factory.register_block("SUBSCRIBE", SubscribeCommand);

        // UNSUBSCRIBE can be both a regular command and an unblock command
        factory.register("UNSUBSCRIBE", UnsubscribeCommand);

        factory
    }

    pub async fn process_connection<RW>(
        &self,
        server: &RedisServer,
        transport: &mut RW,
        mut client: Client,
    ) -> Result<(), std::io::Error>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        loop {
            // 从 reader 读取一条命令
            let value = match transport.next().await {
                Some(Ok(v)) => v,
                Some(Err(e)) => {
                    error!("Read error: {}", e);
                    return Err(e);
                }
                None => return Ok(()), // 连接关闭
            };

            // 解析并执行命令
            if let Err(e) = self
                .execute_command(&mut client, server, transport, value)
                .await
            {
                return Err(e);
            }
        }
    }

    /// 执行单个命令，包括处理阻塞命令的订阅流
    async fn execute_command<RW>(
        &self,
        client: &mut Client,
        server: &RedisServer,
        transport: &mut RW,
        value: Value,
    ) -> Result<(), std::io::Error>
    where
        RW: Stream<Item = Result<Value, std::io::Error>>
            + Sink<Value, Error = std::io::Error>
            + Unpin,
    {
        // 解析命令名
        let (cmd_name, items) = match &value {
            Value::Array(Some(items)) if !items.is_empty() => {
                let name = match &items[0] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => {
                        transport
                            .send(Value::error("invalid command format"))
                            .await?;
                        return Ok(());
                    }
                };
                (name, items.clone())
            }
            _ => {
                transport
                    .send(Value::error("ERR failed to parse command"))
                    .await?;
                return Ok(());
            }
        };

        // 先尝试普通命令
        if let Some(cmd) = self.commands.get(&cmd_name) {
            let resp = match cmd.execute(client, &items, server).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Command '{}' error: {}", cmd_name, e);
                    e.into()
                }
            };
            transport.send(resp).await?;
            return Ok(());
        }

        // 再尝试阻塞命令
        if let Some(cmd) = self.block_commands.get(&cmd_name) {
            match cmd.execute(client, &items, server).await {
                Ok((initial_resp, mut stream)) => {
                    // 发送初始响应
                    transport.send(initial_resp).await?;

                    // 进入阻塞模式：同时监听新命令和订阅流
                    loop {
                        select! {
                            // 订阅流有新数据
                            change = stream.changed() => {
                                match change {
                                    Ok(_) => {
                                        let val = stream.borrow().clone();
                                        match val {
                                            None => return Ok(()), // 订阅结束
                                            Some(v) => {
                                                transport.send(v).await?;
                                            }
                                        }
                                    }
                                    Err(_) => return Ok(()), // 流发送端已关闭
                                }
                            }

                            // 客户端发送了新命令
                            maybe_cmd = transport.next() => {
                                match maybe_cmd {
                                    Some(Ok(value)) => {
                                        // 直接走正常的命令执行链路
                                        // 阻塞命令会检查当前是否在阻塞状态
                                        let (cmd_name, items) = match &value {
                                            Value::Array(Some(items)) if !items.is_empty() => {
                                                let name = match &items[0] {
                                                    Value::BulkString(Some(data)) => {
                                                        String::from_utf8_lossy(data).to_uppercase()
                                                    }
                                                    Value::SimpleString(s) => s.to_uppercase(),
                                                    _ => {
                                                        transport.send(
                                                            Value::error("invalid command format")
                                                        ).await?;
                                                        continue;
                                                    }
                                                };
                                                (name, items.clone())
                                            }
                                            _ => {
                                                transport.send(
                                                    Value::error("ERR failed to parse command")
                                                ).await?;
                                                continue;
                                            }
                                        };
                                        // 检查是否允许在阻塞状态下执行
                                        if cmd.can_handle_unblock(&cmd_name) {
                                            if let Some(cmd) = self.commands.get(&cmd_name) {
                                                let resp = match cmd.execute(client, &items, server).await {
                                                    Ok(v) => v,
                                                    Err(e) => e.into(),
                                                };
                                                transport.send(resp).await?;
                                            }
                                        } else if let Some(cmd) = self.commands.get(&cmd_name) {
                                            transport.send(
                                                Value::error("ERR command not allowed in blocking mode")
                                            ).await?;
                                        } else {
                                            transport.send(
                                                Value::error("ERR unknown command")
                                            ).await?;
                                        }
                                    }
                                    Some(Err(e)) => {
                                        error!("Read error in blocking mode: {}", e);
                                        return Err(e);
                                    }
                                    None => return Ok(()), // 连接关闭
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    transport.send(Value::from(e)).await?;
                }
            }
            return Ok(());
        }

        // 未知命令
        transport.send(Value::error("ERR unknown command")).await?;
        Ok(())
    }
}
