//! HGETALL command implementation
//!
//! HGETALL key
//! Returns all fields and values of the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::types::core::response_value::Value;

/// Parsed HGETALL arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HGetAllParams {
    pub key: Vec<u8>,
}

impl Display for HGetAllParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HGETALL {}",
            String::from_utf8_lossy(&self.key)
        )
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

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(HGetAllParams { key })
    }
}

impl RaftCommand for HGetAllCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HGetAll(params)))
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
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        let params = Self::parse_args(items)?;

        let value = server
            .app
            .read(params.key, client.db_number)
            .await?;

        match value {
            None => Ok(Value::Array(Some(vec![]))),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let mut result = Vec::with_capacity(guard.len() * 2);
                    for (field, value) in guard.iter() {
                        // field
                        result.push(Value::BulkString(Some(field.as_ref().clone())));
                        // value
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };
                        result.push(Value::BulkString(Some(value_bytes)));
                    }
                    Ok(Value::Array(Some(result)))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}