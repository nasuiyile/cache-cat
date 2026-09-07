//! RPOP command implementation
//!
//! RPOP key [count]
//! Remove and return the last element of the list stored at key.
//!
//! Returns:
//! - The last element of the list
//! - Nil if key does not exist
//! - Array of elements when count is specified

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::MochaOperation::Abort;
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::RPop;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// RPOP command handler
pub struct RPopCommand;

impl RPopCommand {
    /// Parse arguments
    /// Format: RPOP key [count]
    fn parse_args(items: &[Value]) -> Result<RPopArgs, ProtocolError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(ProtocolError::WrongArgCount("rpop"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let count = if items.len() == 3 {
            Some(
                items[2]
                    .parse_u64()
                    .ok_or(ProtocolError::InvalidArgument("count"))?,
            )
        } else {
            None
        };
        Ok(RPopArgs { key, count })
    }
}

/// Parsed RPOP arguments
struct RPopArgs {
    key: Bytes,
    count: Option<u64>,
}

impl RaftCommand for RPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(RPop(RPopReq {
            key: params.key,
            count: params.count.unwrap_or(1),
        })))
    }
}

#[async_trait]
impl Command for RPopCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPopReq {
    pub key: Bytes,
    pub count: u64,
}

impl Display for RPopReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RPopReq {{ key: {}, count: {} }}",
            String::from_utf8_lossy(&self.key),
            self.count
        )
    }
}

impl ComputeCommand for RPopReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::RPop(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let popped = {
                    let mut list = data_arc.lock();
                    list.pop_back()  // 主要区别：使用pop_back()而不是pop_front()
                };
                match popped {
                    Some(value) => (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::BulkString(Some(value)),
                    ),
                    None => (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::BulkString(None),
                    ),
                }
            }
            _ => (
                Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (Abort, Value::BulkString(None))
    }
}
