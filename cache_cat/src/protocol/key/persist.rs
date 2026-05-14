//! PERSIST command implementation
//!
//! PERSIST key
//!
//! Remove the existing timeout on key, turning the key from volatile
//! (a key with an expire set) to persistent (a key that will never expire
//! as no timeout is associated).
//!
//! Return values:
//! - `1` if the timeout was removed
//! - `0` if the key does not exist or does not have an associated timeout

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Persist;
use crate::raft::types::entry::bae_operation::PersistReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use std::sync::Arc;

/// PERSIST command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PersistParams {
    pub key: Vec<u8>,
}

impl PersistParams {
    /// Parse PERSIST command parameters from RESP array items
    /// Format: PERSIST key
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("persist"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(PersistParams { key })
    }
}

/// PERSIST command executor
pub struct PersistCommand;

impl RaftCommand for PersistCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = PersistParams::parse(items)?;
        Ok(Operation::Base(Persist(PersistReq {
            key: Arc::from(params.key),
        })))
    }
}

#[async_trait]
impl Command for PersistCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
