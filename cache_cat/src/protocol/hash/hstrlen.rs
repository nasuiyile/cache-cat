//! HSTRLEN command implementation
//!
//! HSTRLEN key field
//! Returns the length of the value associated with field in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HSTRLEN arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HStrLenParams {
    pub key: Bytes,
    pub field: Bytes,
}

impl Display for HStrLenParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HSTRLEN {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field)
        )
    }
}

impl ReadCommand for HStrLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(entry) = value else {
            return Value::Integer(0);
        };

        let ValueObject::Hash(map) = entry.value.data else {
            return CacheCatError::from(ProtocolError::WrongType).into();
        };

        let guard = map.lock();
        let length = match guard.get(&self.field) {
            None => 0,
            Some(HashValue::Str(value)) => value.len(),
            Some(HashValue::Int(value)) => value.to_string().len(),
        };
        Value::Integer(length as i64)
    }
}

/// HSTRLEN command handler.
pub struct HStrLenCommand;

impl HStrLenCommand {
    /// Parse arguments from RESP items.
    /// Format: HSTRLEN key field
    fn parse_args(items: &[Value]) -> Result<HStrLenParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("hstrlen"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let field = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("field"))?;
        Ok(HStrLenParams { key, field })
    }
}

impl ReadRaftCommand for HStrLenCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HStrLen(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HStrLenCommand {
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
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}
