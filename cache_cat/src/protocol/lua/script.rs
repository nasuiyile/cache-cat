use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// SCRIPT LOAD args
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptLoadParams {
    pub script: String,
}

/// SCRIPT EXISTS args
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptExistsParams {
    pub sha1s: Vec<String>,
}

/// SCRIPT FLUSH args (Redis 6+ support ASYNC/SYNC)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScriptFlushParams {
    pub flush_mode: FlushMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FlushMode {
    Sync,
    Async,
}

// SCRIPT DEBUG subcommands
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScriptDebugMode {
    Yes,
    Sync,
    No,
}

/// all SCRIPT enumeration of subcommands
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
                write!(f, "LOAD {}", params.script)
            }
            ScriptParam::Exists(params) => {
                write!(f, "EXISTS {}", params.sha1s.join(" "))
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
    /// Parse SCRIPT sub commands from command array
    /// The first element of the input items must be 'SCRIPT'
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.is_empty() {
            return Err(ProtocolError::WrongArgCount("script"));
        }

        // subcommand name
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
                let script = string_from_value(&items[2], "script")?;
                Ok(ScriptParam::Load(ScriptLoadParams { script }))
            }
            "EXISTS" => {
                // SCRIPT EXISTS sha1 [sha1 ...]
                let mut sha1s = Vec::new();
                for item in &items[2..] {
                    sha1s.push(string_from_value(item, "sha1")?);
                }
                Ok(ScriptParam::Exists(ScriptExistsParams { sha1s }))
            }
            "FLUSH" => {
                // SCRIPT FLUSH [ASYNC | SYNC]
                let mut flush_mode = FlushMode::Sync; // default
                if items.len() > 3 {
                    return Err(ProtocolError::WrongArgCount("script|flush"));
                }
                if items.len() == 3 {
                    let mode_str = string_from_value(&items[2], "flush mode")?.to_uppercase();
                    flush_mode = match mode_str.as_str() {
                        "ASYNC" => FlushMode::Async,
                        "SYNC" => FlushMode::Sync,
                        _ => return Err(ProtocolError::InvalidArgument("flush mode")),
                    };
                }
                Ok(ScriptParam::Flush(ScriptFlushParams { flush_mode }))
            }
            "KILL" => {
                // SCRIPT KILL no arg
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
                let mode_str = string_from_value(&items[2], "debug mode")?.to_uppercase();
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

/// Auxiliary function: Extract string from Value
fn string_from_value(value: &Value, _context: &str) -> Result<String, ProtocolError> {
    match value {
        Value::BulkString(Some(data)) => str::from_utf8(data).map(ToString::to_string).ok(),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
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

        cfg_select! {
            feature = "lua" => {
                use sha1::{Digest, Sha1};

                let value = match param {
                    ScriptParam::Load(v) => {
                        let mut hasher = Sha1::new();
                        hasher.update(&v.script);

                        let hash = hasher.finalize();

                        let sha1_hex = hash
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();

                        server
                            .app
                            .state_machine
                            .data
                            .kvs
                            .lua_env
                            .script_map
                            .lock()
                            .insert(sha1_hex.clone(), v.script);

                        Value::BulkString(Some(sha1_hex.into()))
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
                            Value::Error(String::from("ERR No scripts in execution right now."))
                        }
                    }
                    ScriptParam::Debug(_) => Value::Error("Not implemented".to_string()),
                };
                Ok(value)
            }

            _ => {
                let _ = (server, param);

                // todo!("command handle while `lua` is not enabled")
                Err(ProtocolError::Custom("Command cannot run because `lua` is not enabled").into())
            }
        }
    }
}
