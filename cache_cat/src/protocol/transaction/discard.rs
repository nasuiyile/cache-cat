use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct DiscardCommand;

#[async_trait]
impl Command for DiscardCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // DISCARD does not accept additional parameters
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("DISCARD").into());
        }

        // MULTI must be enabled first
        if client.transaction_queue.is_none() {
            return Err(ProtocolError::response("ERR DISCARD without MULTI").into());
        }

        // Clear transaction queue
        client.transaction_queue = None;

        Ok(Value::ok())
    }
}
