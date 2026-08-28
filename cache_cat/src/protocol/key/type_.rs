//! TYPE command implementation
//!
//! TYPE key
//! Returns the type of the value stored at key.
//!
//! Return value:
//! - Simple string: "string", "list", "set", "zset", "hash", "stream", or "none"
//! - "none" if key does not exist
//! - WRONGTYPE error is not applicable (TYPE always succeeds)

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// TYPE command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeCommand;

/// Parsed arguments for TYPE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeParams {
    pub key: Bytes,
}

impl Display for TypeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TypeParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

/// Convert ValueObject enum to Redis type string
fn value_object_to_type_string(value_obj: &ValueObject) -> &'static str {
    match value_obj {
        ValueObject::String(_) => "string",
        ValueObject::Int(_) => "string",
        ValueObject::List(_) => "list",
        ValueObject::Set(_) => "set",
        ValueObject::ZSet(_) => "zset",
        ValueObject::Hash(_) => "hash",
        ValueObject::Bloom(_) => "MBbloom--",
    }
}

impl ReadCommand for TypeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::SimpleString("none".to_string()),
            Some(v) => {
                let type_str = value_object_to_type_string(&v.value.data);
                Value::SimpleString(type_str.to_string())
            }
        }
    }
}

impl TypeCommand {
    /// Parse TYPE arguments: TYPE key
    fn parse_args(items: &[Value]) -> Result<TypeParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("type"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(TypeParams { key })
    }
}

impl ReadRaftCommand for TypeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Type(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for TypeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
