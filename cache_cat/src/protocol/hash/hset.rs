//! HSET command implementation
//!
//! HSET key field value [field value ...]
//! Sets the specified fields to their respective values in the hash stored at key.
//!
//! Returns:
//! - The number of fields that were added (not updated)
//!
//! Note: This command uses atomic batch write to ensure all fields and metadata
//! are written together as a single atomic operation.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::HSet;
use crate::raft::types::entry::bae_operation::HSetReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetParam {
    key: Bytes,
    fields: Vec<(Bytes, Bytes)>, // (field, value) pairs
}

/// HSET command handler
pub struct HSetCommand;

impl HSetCommand {
    /// Parse arguments from RESP items
    /// Format: HSET key field value [field value ...]
    fn parse_args(items: &[Value]) -> Result<HSetParam, ProtocolError> {
        // Minimum: HSET key field value (4 items)
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("hset"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse field-value pairs from items[2..]
        let field_count = items.len() - 2; // items[2] onwards
        if !field_count.is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("hset"));
        }

        let mut fields = Vec::with_capacity(field_count / 2);
        let mut i = 2;
        while i < items.len() {
            let field = items[i]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("field"))?
                .clone();

            let value = items[i + 1]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("value"))?
                .clone();

            fields.push((field, value));
            i += 2;
        }

        Ok(HSetParam { key, fields })
    }
}

impl RaftCommand for HSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HSet(HSetReq {
            key: params.key,
            elements: params.fields,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HSetCommand {
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
