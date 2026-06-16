//! HKEYS command implementation
//!
//! HKEYS key
//! Returns all field names in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HKEYS arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HKeysParams {
    pub key: Bytes,
}

impl Display for HKeysParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HKEYS {}", String::from_utf8_lossy(&self.key))
    }
}

/// HKEYS command handler
pub struct HKeysCommand;

impl HKeysCommand {
    /// Parse arguments from RESP items
    /// Format: HKEYS key
    fn parse_args(items: &[Value]) -> Result<HKeysParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hkeys"));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(HKeysParams { key: key.into() })
    }
}

impl RaftCommand for HKeysCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HKeys(params)))
    }
}

#[async_trait]
impl Command for HKeysCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        let params = ReadOperation::HKeys(Self::parse_args(items)?);
        server.app.read(params, client.db_number).await
    }
}
