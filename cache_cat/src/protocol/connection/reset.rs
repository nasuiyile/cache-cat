//! RESET command implementation.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// Leave pub/sub mode and clear connection-local state.
pub struct ResetCommand;

#[async_trait]
impl Command for ResetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 1 {
            return Err(ProtocolError::WrongArgCount("reset").into());
        }

        if client.flag.in_sub {
            server.broadcast.remove_client(client.id).await;
        }
        client.flag.in_sub = false;
        client.flag.blocking = false;
        client.transaction_queue = None;
        client.transaction_failed = false;
        client.flag.multi = false;
        client.db_number = 0;
        client.authenticated = server.app.config.password.is_none();
        client.framed.codec_mut().switch_resp2();
        client.name.clear();
        // Redis preserves library metadata across RESET.
        Ok(Value::SimpleString("RESET".to_string()))
    }
}
