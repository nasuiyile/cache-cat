//! Shutdown command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// Shutdown behavior options
#[derive(Debug, PartialEq)]
pub enum ShutdownOption {
    /// Save RDB before shutdown (default)
    Save,
    /// Do not save RDB before shutdown
    NoSave,
}

/// Parsed SHUTDOWN arguments
#[derive(Debug)]
pub struct ShutdownParam {
    pub option: ShutdownOption,
}

impl ShutdownParam {
    /// Parse arguments from RESP items
    /// Format: SHUTDOWN [SAVE|NOSAVE]
    pub fn parse(items: &[Value]) -> Result<ShutdownParam, ProtocolError> {
        match items.len() {
            1 => {
                // SHUTDOWN (no argument, default SAVE)
                Ok(ShutdownParam {
                    option: ShutdownOption::Save,
                })
            }
            2 => {
                // SHUTDOWN with option
                let arg = match &items[1] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => {
                        return Err(ProtocolError::InvalidArgument(
                            "shutdown expects SAVE or NOSAVE argument",
                        ));
                    }
                };

                match arg.as_str() {
                    "SAVE" => Ok(ShutdownParam {
                        option: ShutdownOption::Save,
                    }),
                    "NOSAVE" => Ok(ShutdownParam {
                        option: ShutdownOption::NoSave,
                    }),
                    _ => Err(ProtocolError::InvalidArgument(
                        "shutdown supports only SAVE or NOSAVE arguments",
                    )),
                }
            }
            _ => {
                // Too many arguments
                Err(ProtocolError::WrongArgCount("shutdown"))
            }
        }
    }
}

/// SHUTDOWN command handler
pub struct ShutdownCommand;

#[async_trait]
impl Command for ShutdownCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse arguments first
        let _params = match ShutdownParam::parse(items) {
            Ok(p) => p,
            Err(e) => return Err(e.into()),
        };
        server.app.shutdown().await;
        // Return OK response
        Ok(Value::SimpleString("OK".to_string()))
    }
}
