//! HGETALL command implementation
//!
//! HGETALL key
//! Returns all fields and values of the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use crate::mocha::EntrySnapshot;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::value_object::ValueObject;

/// Parsed HGETALL arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HGetAllParams {
    pub key: Bytes,
}

impl Display for HGetAllParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HGETALL {}", String::from_utf8_lossy(&self.key))
    }
}
impl ReadCommand for HGetAllParams {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Map(Vec::new()),
            Some(v) => match v.value.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let result = guard
                        .iter()
                        .map(|(field, value)| {
                            (
                                Value::BulkString(Some(field.clone())),
                                Value::BulkString(Some(value.to_bytes())),
                            )
                        })
                        .collect::<Vec<_>>();
                    Value::Map(result)
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

/// HGETALL command handler
pub struct HGetAllCommand;

impl HGetAllCommand {
    /// Parse arguments from RESP items
    /// Format: HGETALL key
    fn parse_args(items: &[Value]) -> Result<HGetAllParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hgetall"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        Ok(HGetAllParams { key })
    }
}

impl ReadRaftCommand for HGetAllCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HGetAll(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HGetAllCommand {
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
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
