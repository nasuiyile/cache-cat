//! HLEN command implementation
//!
//! HLEN key
//! Returns the number of fields contained in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use crate::mocha::EntrySnapshot;

/// Parsed HLEN arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HLenParams {
    pub key: Bytes,
}

impl Display for HLenParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HLEN {}", String::from_utf8_lossy(&self.key))
    }
}

impl ReadCommand for HLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Integer(0),
            Some(v) => match v.value.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    Value::Integer(guard.len() as i64)
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

/// HLEN command handler
pub struct HLenCommand;

impl HLenCommand {
    /// Parse arguments from RESP items
    /// Format: HLEN key
    fn parse_args(items: &[Value]) -> Result<HLenParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hlen"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(HLenParams { key })
    }
}

impl RaftCommand for HLenCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HLen(params)))
    }
}

#[async_trait]
impl Command for HLenCommand {
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
        let params = ReadOperation::HLen(Self::parse_args(items)?);
        server.app.read(params, client.db_number).await
    }
}