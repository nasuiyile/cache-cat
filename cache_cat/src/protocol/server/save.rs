//! Save command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use tracing::error;

/// SAVE command handler
pub struct SaveCommand;

#[async_trait]
impl Command for SaveCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("save").into());
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
            //如果已经在快照中了
            return Err(ProtocolError::Custom("Background save already in progress").into());
        }
        //进行快照
        let mut receiver = server.app.state_machine.data.snapshot_message.subscribe();
        let result = server.app.raft.trigger().snapshot().await;
        let _ = receiver.recv().await;
        result.map_err(|e| {
            error!("snapshot error: {}", e);
            ProtocolError::Custom("snapshot error")
        })?;
        Ok(Value::ok())
    }
}
