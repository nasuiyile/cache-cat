//! HMGET command implementation
//!
//! HMGET key field [field ...]
//! Returns the values associated with the specified fields in the hash stored at key.

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

/// Parsed HMGET arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMGetParams {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}

impl Display for HMGetParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fields_str: Vec<String> = self
            .fields
            .iter()
            .map(|f| String::from_utf8_lossy(f).to_string())
            .collect();
        write!(
            f,
            "HMGET {} {}",
            String::from_utf8_lossy(&self.key),
            fields_str.join(" ")
        )
    }
}

/// HMGET command handler
pub struct HMGetCommand;

impl HMGetCommand {
    /// Parse arguments from RESP items
    /// Format: HMGET key field [field ...]
    fn parse_args(items: &[Value]) -> Result<HMGetParams, ProtocolError> {
        // HMGET requires at least key and one field (3 items total)
        let len = items.len();
        if len < 3 {
            return Err(ProtocolError::WrongArgCount("hmget"));
        }

        let mut iter = items.iter().skip(1);

        // Parse key from items[1]
        let key = iter
            .next()
            .expect("there will not be none")
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse all fields (starting from index 2)
        let fields = iter
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if fields.len() < len - 2 {
            return Err(ProtocolError::InvalidArgument("field"));
        }

        Ok(HMGetParams { key, fields })
    }
}

impl RaftCommand for HMGetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HMGet(params)))
    }
}

#[async_trait]
impl Command for HMGetCommand {
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

        // Parse arguments
        let params = Self::parse_args(items)?;
        server
            .app
            .read(ReadOperation::HMGet(params), client.db_number)
            .await
    }
}
