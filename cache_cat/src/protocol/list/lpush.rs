//! LPUSH command implementation
//!
//! LPUSH key element [element ...]
//! Insert all the specified values at the head of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `LPUSH mylist a b c` results in `[c, b, a]`.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::LPush;
use crate::raft::types::entry::bae_operation::LPushReq;
use async_trait::async_trait;
use std::sync::Arc;

/// LPUSH command handler
pub struct LPushCommand;

impl LPushCommand {
    /// Parse arguments from RESP items
    /// Format: LPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<LPushArgs, ProtocolError> {
        // Minimum: LPUSH key element (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("lpush"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse elements from items[2..]
        let mut elements = Vec::with_capacity(items.len() - 2);
        for item in &items[2..] {
            let elem = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("element")),
            };
            elements.push(elem);
        }

        Ok(LPushArgs { key, elements })
    }
}

/// Parsed LPUSH arguments
struct LPushArgs {
    key: Vec<u8>,
    elements: Vec<Vec<u8>>,
}

#[async_trait]
impl Command for LPushCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse arguments
        let params = Self::parse_args(items)?;
        let mut elements = Vec::new();
        for v in params.elements {
            elements.push(Arc::new(v));
        }
        let operation = LPush(LPushReq {
            key: Arc::from(params.key),
            elements,
        });
        let value = server.app.write_base(operation, *db_number).await?;
        Ok(value)
    }
}
