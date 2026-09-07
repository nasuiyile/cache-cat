//! UNLINK command implementation
//!
//! UNLINK key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//! The actual memory reclamation is performed asynchronously.
//!
//! Returns:
//! - The number of keys that were unlinked
//! - 0 if none of the specified keys existed

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Unlink;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisUnlink;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

/// UNLINK command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnlinkParams {
    pub keys: Vec<Bytes>,
}

impl UnlinkParams {
    /// Parse UNLINK command parameters from RESP array items
    /// Format: UNLINK key [key ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: UNLINK key
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("unlink"));
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 1 {
            return Err(ProtocolError::WrongArgCount("unlink"));
        }

        Ok(UnlinkParams { keys })
    }
}

impl Display for UnlinkParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnlinkReq {{ keys: {:?} }}", self.keys)
    }
}

/// UNLINK command executor
pub struct UnlinkCommand;

impl RaftCommand for UnlinkCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = UnlinkParams::parse(items)?;

        let operation = if params.keys.len() == 1 {
            Operation::Base(Unlink(UnlinkReq {
                key: params.keys[0].clone(),
            }))
        } else {
            Operation::Redis(RedisUnlink(params))
        };

        Ok(operation)
    }
}

#[async_trait]
impl Command for UnlinkCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnlinkReq {
    pub key: Bytes,
}

impl Display for UnlinkReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UnlinkReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}