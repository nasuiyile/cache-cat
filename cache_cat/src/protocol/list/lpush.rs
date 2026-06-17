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
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::LPush;
use crate::raft::types::entry::bae_operation::LPushReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// LPUSH command handler
pub struct LPushCommand;

impl LPushCommand {
    /// Parse arguments from RESP items
    /// Format: LPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<LPushArgs, ProtocolError> {
        // Minimum: LPUSH key element (3 items)
        let len = items.len();
        if len < 3 {
            return Err(ProtocolError::WrongArgCount("lpush"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse elements from items[2..]
        let elements = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if elements.len() < len - 2 {
            return Err(ProtocolError::InvalidArgument("element"));
        }

        Ok(LPushArgs { key, elements })
    }
}

/// Parsed LPUSH arguments
struct LPushArgs {
    key: Bytes,
    elements: Vec<Bytes>,
}

impl RaftCommand for LPushCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(LPush(LPushReq {
            key: params.key,
            elements: params.elements,
        })))
    }
}

#[async_trait]
impl Command for LPushCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
