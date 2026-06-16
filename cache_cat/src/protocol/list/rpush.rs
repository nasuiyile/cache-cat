//! RPUSH command implementation
//!
//! RPUSH key element [element ...]
//! Insert all the specified values at the tail of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `RPUSH mylist a b c` results in `[a, b, c]`.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::RPush;
use crate::raft::types::entry::bae_operation::RPushReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

/// RPUSH command handler
pub struct RPushCommand;

impl RPushCommand {
    /// Parse arguments from RESP items
    /// Format: RPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<RPushArgs, ProtocolError> {
        // Minimum: RPUSH key element
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("rpush"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse elements
        let mut elements = Vec::with_capacity(items.len() - 2);

        for item in &items[2..] {
            let elem = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("element")),
            };

            elements.push(elem);
        }

        Ok(RPushArgs {
            key: key.into(),
            elements,
        })
    }
}

/// Parsed RPUSH arguments
struct RPushArgs {
    key: Bytes,
    elements: Vec<Vec<u8>>,
}

impl RaftCommand for RPushCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        let mut elements = Vec::new();
        for v in params.elements {
            elements.push(Arc::new(v));
        }

        Ok(Operation::Base(RPush(RPushReq {
            key: params.key,
            elements,
        })))
    }
}

#[async_trait]
impl Command for RPushCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // MULTI transaction support
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        // Execute through Raft
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}
