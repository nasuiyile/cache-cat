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
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::Persist;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// PERSIST command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PersistParams {
    pub key: Bytes,
}

impl PersistParams {
    /// Parse PERSIST command parameters from RESP array items
    /// Format: PERSIST key
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("persist"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(PersistParams { key })
    }
}

/// PERSIST command executor
pub struct PersistCommand;

impl RaftCommand for PersistCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = PersistParams::parse(items)?;
        Ok(Operation::Base(Persist(PersistReq { key: params.key })))
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
            return Ok(Value::queued());
        }
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistReq {
    pub key: Bytes,
}

impl fmt::Display for PersistReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PersistReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ComputeCommand for PersistReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Persist(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        if entry.expire_at.is_none() {
            return (MochaOperation::Abort, Value::Boolean(false));
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: ExpirePolicy::Persistent,
            },
            Value::Boolean(true),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Boolean(false))
    }
}
