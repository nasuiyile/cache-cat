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
            return Err(ProtocolError::WrongArgCount("AUTH").into());
        }

        let password = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::SyntaxError)?
            .into_owned();

        let configured_password =
            server
                .app
                .config
                .password
                .as_ref()
                .ok_or(ProtocolError::response(
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
                ))?;

        if password == *configured_password {
            client.authenticated = true;
            Ok(Value::ok())
        } else {
            Err(ProtocolError::AuthenticationFailed.into())
        }
    }
}
