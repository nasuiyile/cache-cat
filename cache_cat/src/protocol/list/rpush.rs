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

/// RPUSH command handler
pub struct RPushCommand;

impl RPushCommand {
    /// Parse arguments from RESP items
    /// Format: RPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<RPushArgs, ProtocolError> {
        // Minimum: RPUSH key element
        let len = items.len();
        if len < 3 {
            return Err(ProtocolError::WrongArgCount("rpush"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse elements
        let elements = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if elements.len() < len - 2 {
            return Err(ProtocolError::InvalidArgument("element"));
        }

        Ok(RPushArgs { key, elements })
    }
}

/// Parsed RPUSH arguments
struct RPushArgs {
    key: Bytes,
    elements: Vec<Bytes>,
}

impl RaftCommand for RPushCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(RPush(RPushReq {
            key: params.key,
            elements: params.elements,
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
            return Ok(Value::from_static_string("QUEUED"));
        }

        // Execute through Raft
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}
