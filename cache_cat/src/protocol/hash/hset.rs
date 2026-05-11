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
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::HSet;
use crate::raft::types::entry::bae_operation::HSetReq;
use async_trait::async_trait;
use std::sync::Arc;

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetParam {
    key: Vec<u8>,
    fields: Vec<(Vec<u8>, Vec<u8>)>, // (field, value) pairs
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
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse field-value pairs from items[2..]
        let field_count = items.len() - 2; // items[2] onwards
        if !field_count.is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("hset"));
        }

        let mut fields = Vec::with_capacity(field_count / 2);
        let mut i = 2;
        while i < items.len() {
            let field = match &items[i] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("field")),
            };

            let value = match &items[i + 1] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("value")),
            };

            fields.push((field, value));
            i += 2;
        }

        Ok(HSetParam { key, fields })
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
        // Parse arguments
        let params = Self::parse_args(items)?;
        let mut vec = Vec::new();
        for v in params.fields {
            vec.push((Arc::new(v.0), Arc::new(v.1)));
        }
        let operation = HSet(HSetReq {
            key: Arc::from(params.key),
            elements: vec,
        });
        let value = server.app.write_base(operation, client.db_number).await?;
        Ok(value)
    }
}
