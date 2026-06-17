//! LPOP command implementation
//!
//! LPOP key [count]
//! Remove and return the first element of the list stored at key.
//!
//! Returns:
//! - The first element of the list
//! - Nil if key does not exist
//! - Array of elements when count is specified

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::LPop;
use crate::raft::types::entry::bae_operation::LPopReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// LPOP command handler
pub struct LPopCommand;

impl LPopCommand {
    /// Parse arguments
    /// Format: LPOP key [count]
    fn parse_args(items: &[Value]) -> Result<LPopArgs, ProtocolError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(ProtocolError::WrongArgCount("lpop"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let count = if items.len() == 3 {
            let count = items[2]
                .parse_u64()
                .ok_or(ProtocolError::InvalidArgument("count"))?;

            Some(count)
        } else {
            None
        };

        Ok(LPopArgs { key, count })
    }
}

/// Parsed LPOP arguments
struct LPopArgs {
    key: Bytes,
    count: Option<u64>,
}

impl RaftCommand for LPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(LPop(LPopReq {
            key: params.key,
            count: params.count.unwrap_or(1),
        })))
    }
}

#[async_trait]
impl Command for LPopCommand {
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
