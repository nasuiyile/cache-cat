use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for DBSIZE command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbsizeParams {
    pub keys: Vec<Bytes>,
}

impl Display for DbsizeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DBSIZE")
    }
}

impl DbsizeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 1 {
            return Err(ProtocolError::WrongArgCount("DBSIZE"));
        }

        Ok(Self {
            keys: Vec::new(),
        })
    }
}

/// DBSIZE command executor
pub struct DbsizeCommand;

impl ReadRaftCommand for DbsizeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::DbSize(DbsizeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for DbsizeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}