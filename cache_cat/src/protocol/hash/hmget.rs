//! HMGET command implementation
//!
//! HMGET key field [field ...]
//! Returns the values associated with the specified fields in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use crate::mocha::EntrySnapshot;

/// Parsed HMGET arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMGetParams {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}
impl Display for HMGetParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let fields_str: Vec<String> = self
            .fields
            .iter()
            .map(|f| String::from_utf8_lossy(f).to_string())
            .collect();
        write!(
            f,
            "HMGET {} {}",
            String::from_utf8_lossy(&self.key),
            fields_str.join(" ")
        )
    }
}
impl ReadCommand for HMGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.value.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let results: Vec<Value> = self
                        .fields
                        .iter()
                        .map(|field| match guard.get(field) {
                            None => Value::BulkString(None),
                            Some(value) => match value {
                                HashValue::Str(str) => Value::BulkString(Some(str.clone())),
                                HashValue::Int(int) => {
                                    Value::BulkString(Some(int.to_string().into()))
                                }
                            },
                        })
                        .collect();
                    Value::Array(Some(results))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

/// HMGET command handler
pub struct HMGetCommand;

impl HMGetCommand {
    /// Parse arguments from RESP items
    /// Format: HMGET key field [field ...]
    fn parse_args(items: &[Value]) -> Result<HMGetParams, ProtocolError> {
        // HMGET requires at least key and one field (3 items total)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hmget"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let fields = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();
        if fields.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("field"));
        }
        Ok(HMGetParams { key, fields })
    }
}

impl RaftCommand for HMGetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HMGet(params)))
    }
}

#[async_trait]
impl Command for HMGetCommand {
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

        // Parse arguments
        let params = Self::parse_args(items)?;
        server
            .app
            .read(ReadOperation::HMGet(params), client.db_number)
            .await
    }
}
