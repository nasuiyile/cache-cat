//! ZCARD command implementation
//!
//! ZCARD key
//! Returns the number of members in the sorted set stored at key.
//!
//! Return value:
//! - Integer reply: number of elements
//! - 0 if key does not exist
//! - WRONGTYPE if key exists but is not a sorted set

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// ZCARD command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZCardCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZCardParams {
    pub key: Bytes,
}

impl ReadCommand for ZCardParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // key 不存在
            None => Value::Integer(0),

            Some(v) => {
                match v.value.data {
                    ZSet(zset) => {
                        let len = zset.lock().len();

                        Value::Integer(len as i64)
                    }

                    // 类型不是 zset
                    _ => ProtocolError::WrongType.into(),
                }
            }
        }
    }
}

impl Display for ZCardParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZCardParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
impl ZCardCommand {
    fn parse_args(items: &[Value]) -> Result<ZCardParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("zcard"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        Ok(ZCardParams { key })
    }
}

impl ReadRaftCommand for ZCardCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZCard(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZCardCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let params = self.read_operation(items)?;

        server.app.read(params, client.db_number).await
    }
}
