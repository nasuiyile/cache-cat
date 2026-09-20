//! Save command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

/// SAVE command handler
pub struct BgsaveCommand;

#[async_trait]
impl Command for BgsaveCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() > 2 {
            return Err(ProtocolError::SyntaxError.into());
        }
        if items.len() == 2 {
            let arg = match &items[1] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                Value::SimpleString(s) => s.to_uppercase(),
                _ => return Err(CacheCatError::from(ProtocolError::SyntaxError)),
            };
            if arg != "SCHEDULE" {
                return Err(ProtocolError::SyntaxError.into());
            }
        }
        let snapshot_state = server
            .app
            .state_machine
            .data
            .raft_meta_data
            .lock()
            .await
            .snapshot_state();
        if snapshot_state {
            // SCHEDULE does not queue another save behind an active snapshot.
            return Err(ProtocolError::response("ERR Background save already in progress").into());
        }
        server.app.cluster.trigger_snapshot().await?;

        Ok(Value::SimpleString("Background saving started".to_string()))
    }
}
