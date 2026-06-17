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
use bytes::Bytes;

/// Parsed HDEL arguments
#[derive(Debug)]
struct HDelParam {
    key: Bytes,
    fields: Vec<Bytes>, // fields to delete
}

/// HDEL command handler
pub struct HDelCommand;

impl HDelCommand {
    /// Parse arguments from RESP items
    /// Format: HDEL key field [field ...]
    fn parse_args(items: &[Value]) -> Result<HDelParam, ProtocolError> {
        // Minimum: HDEL key field (3 items)
        let len = items.len();
        if len < 3 {
            return Err(ProtocolError::WrongArgCount("hdel"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse fields from items[2..]
        let fields = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if fields.len() < len - 2 {
            return Err(ProtocolError::InvalidArgument("field"));
        }

        Ok(HDelParam { key, fields })
    }
}

impl RaftCommand for HDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HDel(HDelReq {
            key: params.key,
            fields: params.fields,
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
            return Ok(Value::from_static_string("QUEUED"));
        }
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
