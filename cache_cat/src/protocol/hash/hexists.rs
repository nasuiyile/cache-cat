//! HEXISTS command implementation
//!
//! HEXISTS key field
//! Returns if field is an existing field in the hash stored at key.

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

/// Parsed HEXISTS arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HExistsParams {
    pub key: Bytes,
    pub field: Bytes,
}

impl Display for HExistsParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HEXISTS {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field)
        )
    }
}

impl ReadCommand for HExistsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Integer(0),
            Some(v) => match v.value.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    if guard.contains_key(&self.field) {
                        Value::Integer(1)
                    } else {
                        Value::Integer(0)
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

/// HEXISTS command handler
pub struct HExistsCommand;

impl HExistsCommand {
    /// Parse arguments from RESP items
    /// Format: HEXISTS key field
    fn parse_args(items: &[Value]) -> Result<HExistsParams, ProtocolError> {
        // HEXISTS key field (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hexists"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // Parse field
        let field = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("field"))?;

        Ok(HExistsParams { key, field })
    }
}

impl ReadRaftCommand for HExistsCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HExists(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HExistsCommand {
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
        // Parse arguments and execute read operation
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}