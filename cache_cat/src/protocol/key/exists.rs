//! EXISTS command implementation
//!
//! EXISTS key [key ...]
//! Returns the number of keys that exist from those specified as arguments.

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

/// EXISTS command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExistsParams {
    pub keys: Vec<Bytes>,
}

impl Display for ExistsParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExistsParams {{ keys: {:?} }}", self.keys)
    }
}

impl ExistsParams {
    /// Parse EXISTS command parameters from RESP array items
    /// Format: EXISTS key [key ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: EXISTS key (2 items)
        let len = items.len();
        if len < 2 {
            return Err(ProtocolError::WrongArgCount("exists"));
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if keys.len() < len - 1 {
            return Err(ProtocolError::InvalidArgument("exists"));
        }

        Ok(ExistsParams { keys })
    }
}

impl ReadRaftCommand for ExistsCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Exists(ExistsParams::parse(items)?))
    }
}

/// EXISTS command executor
pub struct ExistsCommand;

#[async_trait]
impl Command for ExistsCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}
