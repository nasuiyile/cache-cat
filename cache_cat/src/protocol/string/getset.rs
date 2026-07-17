use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisGetSet;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetSetParams {
    /// The key to get and set
    pub key: Bytes,
    /// The new value to set
    pub value: Bytes,
}

impl fmt::Display for GetSetParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GetSet {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

/// GETSET command executor
pub struct GetSetCommand;

impl GetSetCommand {
    /// Parse GETSET command parameters
    ///
    /// Format:
    /// GETSET key value
    fn parse(items: &[Value]) -> Result<GetSetParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("getset"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let value = items[2]  // Fixed: using items[2] instead of items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(GetSetParams { key, value })
    }
}

impl RaftCommand for GetSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Redis(RedisGetSet(params)))
    }
}

#[async_trait]
impl Command for GetSetCommand {
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
        let params = Self::parse(items)?;
        server
            .app
            .write(Operation::Redis(RedisGetSet(params)), client.db_number)
            .await
    }
}