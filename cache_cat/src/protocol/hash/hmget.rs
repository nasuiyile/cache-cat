//! HMGET command implementation
//!
//! HMGET key field [field ...]
//! Returns the values associated with the specified fields in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HMGET arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMGetParams {
    pub key: Vec<u8>,
    pub fields: Vec<Vec<u8>>,
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

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse all fields (starting from index 2)
        let mut fields = Vec::with_capacity(items.len() - 2);
        for item in &items[2..] {
            let field = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("field")),
            };
            fields.push(field);
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
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        // Parse arguments
        let params = Self::parse_args(items)?;
        let value = server.app.read(params.key, client.db_number).await?;

        match value {
            None => {
                // Key doesn't exist, return array of null bulk strings
                let nulls: Vec<Value> = params
                    .fields
                    .iter()
                    .map(|_| Value::BulkString(None))
                    .collect();
                Ok(Value::Array(Some(nulls)))
            }
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let results: Vec<Value> = params
                        .fields
                        .iter()
                        .map(|field| match guard.get(field) {
                            None => Value::BulkString(None),
                            Some(value) => match value {
                                HashValue::Str(str) => {
                                    Value::BulkString(Some(str.as_ref().clone()))
                                }
                                HashValue::Int(int) => {
                                    Value::BulkString(Some(int.to_string().as_bytes().to_vec()))
                                }
                            },
                        })
                        .collect();
                    Ok(Value::Array(Some(results)))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
