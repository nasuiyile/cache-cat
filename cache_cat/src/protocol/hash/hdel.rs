//! HDEL command implementation
//!
//! HDEL key field [field ...]
//! Deletes one or more fields from the hash stored at key.
//! Specified fields that do not exist within this hash are ignored.
//!
//! Returns:
//! - The number of fields that were removed from the hash, not including
//!   specified but non-existing fields
//!
//! Note: This command uses atomic batch write to ensure all field deletions
//! and metadata updates are written together as a single atomic operation.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::HDel;
use crate::raft::types::entry::bae_operation::HDelReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use std::sync::Arc;

/// Parsed HDEL arguments
#[derive(Debug)]
struct HDelParam {
    key: Vec<u8>,
    fields: Vec<Vec<u8>>, // fields to delete
}

/// HDEL command handler
pub struct HDelCommand;

impl HDelCommand {
    /// Parse arguments from RESP items
    /// Format: HDEL key field [field ...]
    fn parse_args(items: &[Value]) -> Result<HDelParam, ProtocolError> {
        // Minimum: HDEL key field (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hdel"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse fields from items[2..]
        let mut fields = Vec::with_capacity(items.len() - 2);
        for i in 2..items.len() {
            let field = match &items[i] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("field")),
            };
            fields.push(field);
        }

        Ok(HDelParam { key, fields })
    }
}

impl RaftCommand for HDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let fields: Vec<Arc<Vec<u8>>> = params.fields.into_iter().map(Arc::from).collect();
        let operation = HDel(HDelReq {
            key: Arc::from(params.key),
            fields,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HDelCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
