//! Save command implementation

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use tracing::error;

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
            return Err(ProtocolError::WrongArgCount("save").into());
        }
        let mut schedule = false;
        if items.len() == 2 {
            let arg = match &items[1] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                Value::SimpleString(s) => s.to_uppercase(),
                _ => return Err(CacheCatError::from(ProtocolError::SyntaxError)),
            };
            if arg == "SCHEDULE" {
                schedule = true;
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
        if snapshot_state && (!schedule) {
            //如果已经在快照中了
            return Err(ProtocolError::Custom("Background save already in progress").into());
        }
        let mut receiver = server.app.state_machine.data.snapshot_message.subscribe();

        let result = server.app.raft.trigger().snapshot().await;
        result.map_err(|e| {
            error!("snapshot error: {}", e);
            ProtocolError::Custom("snapshot error")
        })?;
        //在快照执行完毕之后再执行一次
        if schedule && snapshot_state {
            let app = server.app.clone();
            tokio::task::spawn(async move {
                _ = receiver.recv().await;
                let result = app.raft.trigger().snapshot().await;
                _ = result.map_err(|e| {
                    error!("snapshot error: {}", e);
                });
            });
        }

        Ok(Value::ok())
    }
}
