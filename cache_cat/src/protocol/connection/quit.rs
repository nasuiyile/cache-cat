use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use tracing::error;

/// SAVE command handler
pub struct QuitCommand;

#[async_trait]
impl Command for QuitCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("QUIT").into());
        }
        client.closed = true;
        Ok(Value::ok())
    }
}
