use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::fmt;

/// SCRIPT LOAD 的参数
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptLoadParams {
    pub script: Bytes,
}

impl ScriptLoadParams {
    #[inline]
    pub fn script(&self) -> Option<&str> {
        str::from_utf8(&self.script).ok()
    }
}

/// SCRIPT EXISTS 的参数
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptExistsParams {
    pub sha1s: Vec<Bytes>,
}

/// SCRIPT FLUSH 的参数 (Redis 6+ 支持 ASYNC/SYNC)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptFlushParams {
    pub flush_mode: FlushMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FlushMode {
    Sync,
    Async,
}

/// SCRIPT DEBUG 的子命令
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScriptDebugMode {
    Yes,
    Sync,
    No,
}

/// 所有 SCRIPT 子命令的枚举
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScriptParam {
    Load(ScriptLoadParams),
    Exists(ScriptExistsParams),
    Flush(ScriptFlushParams),
    Kill,
    Debug(ScriptDebugMode),
}

impl fmt::Display for ScriptParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScriptParam::Load(params) => {
                write!(f, "LOAD {}", params.script().ok_or(fmt::Error)?)
            }
            ScriptParam::Exists(params) => {
                let sha1s = params
                    .sha1s
                    .iter()
                    .map_while(|bytes| str::from_utf8(bytes).ok())
                    .collect::<Vec<_>>();

                if sha1s.len() < params.sha1s.len() {
                    return Err(fmt::Error);
                }

                write!(f, "EXISTS {}", sha1s.join(" "))
            }
            ScriptParam::Flush(params) => match params.flush_mode {
                FlushMode::Sync => write!(f, "FLUSH SYNC"),
                FlushMode::Async => write!(f, "FLUSH ASYNC"),
            },
            ScriptParam::Kill => {
                write!(f, "KILL")
            }
            ScriptParam::Debug(mode) => match mode {
                ScriptDebugMode::Yes => write!(f, "DEBUG YES"),
                ScriptDebugMode::Sync => write!(f, "DEBUG SYNC"),
                ScriptDebugMode::No => write!(f, "DEBUG NO"),
            },
        }
    }
}

impl ScriptParam {
    /// 从命令数组解析 SCRIPT 子命令
    /// 输入 items 的第一个元素必须是 "SCRIPT"
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.is_empty() {
            return Err(ProtocolError::WrongArgCount("script"));
        }

        // 子命令名
        let sub_cmd = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("script subcommand"))?
            .to_uppercase();

        match sub_cmd.as_str() {
            "LOAD" => {
                // SCRIPT LOAD script
                if items.len() != 3 {
                    return Err(ProtocolError::WrongArgCount("script|load"));
                }
                let script = string_from_value(&items[2], "script")?.clone();
                Ok(ScriptParam::Load(ScriptLoadParams { script }))
            }
            "EXISTS" => {
                // SCRIPT EXISTS sha1 [sha1 ...]
                let mut sha1s = Vec::new();
                for item in &items[2..] {
                    sha1s.push(string_from_value(item, "sha1")?.clone());
                }
                Ok(ScriptParam::Exists(ScriptExistsParams { sha1s }))
            }
            "FLUSH" => {
                // SCRIPT FLUSH [ASYNC | SYNC]
                let mut flush_mode = FlushMode::Sync; // 默认
                if items.len() > 3 {
                    return Err(ProtocolError::WrongArgCount("script|flush"));
                }
                if items.len() == 3 {
                    let mode_str = uppercase_string_from_value(&items[2], "flush mode")?;
                    flush_mode = match mode_str.as_str() {
                        "ASYNC" => FlushMode::Async,
                        "SYNC" => FlushMode::Sync,
                        _ => return Err(ProtocolError::InvalidArgument("flush mode")),
                    };
                }
                Ok(ScriptParam::Flush(ScriptFlushParams { flush_mode }))
            }
            "KILL" => {
                // SCRIPT KILL 无参数
                if items.len() != 2 {
                    return Err(ProtocolError::WrongArgCount("script|kill"));
                }
                Ok(ScriptParam::Kill)
            }
            "DEBUG" => {
                // SCRIPT DEBUG YES|SYNC|NO
                if items.len() != 3 {
                    return Err(ProtocolError::WrongArgCount("script|debug"));
                }
                let mode_str = uppercase_string_from_value(&items[2], "debug mode")?;
                let mode = match mode_str.as_str() {
                    "YES" => ScriptDebugMode::Yes,
                    "SYNC" => ScriptDebugMode::Sync,
                    "NO" => ScriptDebugMode::No,
                    _ => return Err(ProtocolError::InvalidArgument("debug mode")),
                };
                Ok(ScriptParam::Debug(mode))
            }
            _ => Err(ProtocolError::InvalidArgument("unknown script subcommand")),
        }
    }
}

/// 辅助函数：从 Value 提取字符串
fn string_from_value<'a>(value: &'a Value, _context: &str) -> Result<&'a Bytes, ProtocolError> {
    value
        .string_bytes()
        .ok_or(ProtocolError::InvalidArgument("script"))
}

fn uppercase_string_from_value(value: &Value, _context: &str) -> Result<String, ProtocolError> {
    value
        .as_str()
        .map(|str| str.to_uppercase())
        .ok_or(ProtocolError::InvalidArgument("script"))
}

pub struct ScriptCommand;

#[async_trait]
impl Command for ScriptCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let param = ScriptParam::parse(items)?;
        let value = match param {
            ScriptParam::Load(v) => {
                let mut hasher = Sha1::new();
                hasher.update(&v.script);

                let hash = hasher.finalize();

                // TODO: optimize
                let sha1_hex: Bytes = hash
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()
                    .into();

                server
                    .app
                    .state_machine
                    .data
                    .kvs
                    .lua_env
                    .script_map
                    .lock()
                    .insert(sha1_hex.clone(), v.script);

                Value::BulkString(Some(sha1_hex))
            }
            ScriptParam::Exists(v) => {
                let map = server.app.state_machine.data.kvs.lua_env.script_map.lock();
                let mut exists = Vec::new();
                for sha in v.sha1s {
                    if map.contains_key(&sha) {
                        exists.push(Value::Integer(1));
                    } else {
                        exists.push(Value::Integer(0));
                    }
                }
                Value::Array(Some(exists))
            }
            ScriptParam::Flush(mode) => match mode.flush_mode {
                FlushMode::Sync => {
                    server
                        .app
                        .state_machine
                        .data
                        .kvs
                        .lua_env
                        .script_map
                        .lock()
                        .clear();
                    Value::ok()
                }
                FlushMode::Async => {
                    server
                        .app
                        .state_machine
                        .data
                        .kvs
                        .lua_env
                        .script_map
                        .lock()
                        .clear();
                    Value::ok()
                }
            },
            ScriptParam::Kill => {
                let executor = server.app.state_machine.data.kvs.lua_env.interrupt();
                if executor {
                    Value::ok()
                } else {
                    Value::Error(Bytes::from_static(
                        b"ERR No scripts in execution right now.",
                    ))
                }
            }
            ScriptParam::Debug(_) => Value::Error(Bytes::from_static(b"Not implemented")),
        };
        Ok(value)
    }
}
