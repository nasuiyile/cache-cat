//! HINCRBY command implementation
//!
//! HINCRBY key field increment
//! Increments the integer value of a field in a hash by a number.
//! Uses 0 as initial value if the field doesn't exist.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::HIncr;
use crate::raft::types::entry::bae_operation::HIncrReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// Parsed HINCRBY arguments
#[derive(Debug)]
struct HIncrByParams {
    key: Bytes,
    field: Bytes,
    increment: i64,
}

/// HINCRBY command handler
pub struct HIncrByCommand;

impl HIncrByCommand {
    /// Parse arguments from RESP items
    /// Format: HINCRBY key field increment
    fn parse_args(items: &[Value]) -> Result<HIncrByParams, ProtocolError> {
        // HINCRBY key field increment (4 items)
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("hincrby"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        // Parse field
        let field = items[2]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("field"))?
            .clone();

        // Parse increment
        let increment = items[3].try_parse_i64()?;

        Ok(HIncrByParams {
            key,
            field,
            increment,
        })
    }
}

impl RaftCommand for HIncrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HIncr(HIncrReq {
            key: params.key,
            field: params.field,
            value: params.increment,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HIncrByCommand {
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
