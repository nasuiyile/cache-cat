//! HVALS command implementation
//!
//! HVALS key
//! Returns all values in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HVALS arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HValsParams {
    pub key: Bytes,
}

impl Display for HValsParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HVALS {}", String::from_utf8_lossy(&self.key))
    }
}

/// HVALS command handler
pub struct HValsCommand;

impl HValsCommand {
    /// Parse arguments from RESP items
    /// Format: HVALS key
    fn parse_args(items: &[Value]) -> Result<HValsParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hvals"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        Ok(HValsParams { key })
    }
}

impl ReadRaftCommand for HValsCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HVals(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HValsCommand {
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
        server.app.read(params, client.db_number).await
    }
}
