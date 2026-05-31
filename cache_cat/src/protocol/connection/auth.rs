//! AUTH command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// AUTH command handler
pub struct AuthCommand;

#[async_trait]
impl Command for AuthCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("auth").into());
        }

        let password = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(CacheCatError::from(ProtocolError::SyntaxError)),
        };

        let configured_password = match &server.app.config.password {
            Some(p) => p,
            None => {
                return Err(
                    ProtocolError::Custom("AUTH called without any password configured").into(),
                );
            }
        };

        if password == *configured_password {
            client.authenticated = true;
            Ok(Value::ok())
        } else {
            Err(ProtocolError::Custom("invalid username-password pair").into())
        }
    }
}
