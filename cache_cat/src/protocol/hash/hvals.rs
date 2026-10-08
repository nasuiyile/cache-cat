//! HVALS command implementation
//!
//! HVALS key
//! Returns all values in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HVALS arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HValsParams {
    pub key: Bytes,
}

impl Display for HValsParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HVALS {}", String::from_utf8_lossy(&self.key))
    }
}

impl ReadCommand for HValsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.value.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();

                    let result = guard
                        .values()
                        .map(|v| Value::BulkString(Some(v.to_bytes())))
                        .collect::<Vec<_>>();

                    Value::Array(Some(result))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, _read_clock: u64) -> Value {
        let Some(entry) = value else {
            return Value::Array(Some(Vec::new()));
        };
        let ValueObject::Hash(map) = entry.value.data else {
            return CacheCatError::from(ProtocolError::WrongType).into();
        };
        let mut fields = map
            .lock()
            .iter()
            .map(|(field, value)| (field.clone(), value.to_bytes()))
            .collect::<Vec<_>>();
        // Use the same field order as HKEYS/HGETALL, including binary fields.
        fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        Value::Array(Some(
            fields
                .into_iter()
                .map(|(_, value)| Value::BulkString(Some(value)))
                .collect(),
        ))
    }
}

/// HVALS command handler
pub struct HValsCommand;

impl HValsCommand {
    fn parse_args(items: &[Value]) -> Result<HValsParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("hvals"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(HValsParams { key })
    }
}

impl ReadRaftCommand for HValsCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HVals(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HValsCommand {
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
