//! LSET command implementation
//!
//! LSET key index value
//! Sets the list element at index to value.
//! Index is zero-based, with negative indices indicating elements starting at the end of the list.
//!
//! Returns:
//! - Simple string "OK" on success
//!
//! Errors:
//! - If the key does not exist, returns an error
//! - If the key exists but is not a list, returns an error
//! - If the index is out of range, returns an error

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::LSet;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// LSET command handler
pub struct LSetCommand;

impl LSetCommand {
    /// Parse arguments from RESP items
    /// Format: LSET key index value
    fn parse_args(items: &[Value]) -> Result<LSetArgs, ProtocolError> {
        // LSET key index value (4 items including command name)
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("lset"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let index_str = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("index"))?;
        let index_str = String::from_utf8_lossy(&index_str);
        let index = index_str
            .parse::<i64>()
            .map_err(|_| ProtocolError::InvalidArgument("index must be an integer"))?;
        let value = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(LSetArgs { key, index, value })
    }
}

/// Parsed LSET arguments
struct LSetArgs {
    key: Bytes,
    index: i64,
    value: Bytes,
}

impl RaftCommand for LSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(LSet(LSetReq {
            key: params.key,
            index: params.index,
            value: params.value,
        })))
    }
}

#[async_trait]
impl Command for LSetCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LSetReq {
    pub key: Bytes,
    pub index: i64,
    pub value: Bytes,
}

impl Display for LSetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LSetReq {{ key: {}, index: {}, value: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.index,
            self.value
        )
    }
}

impl ComputeCommand for LSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LSet(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let mut list = data_arc.lock();
                let len = list.len() as i64;

                // 处理负索引
                let index = if self.index < 0 {
                    len + self.index
                } else {
                    self.index
                };
                // 检查索引是否有效
                if index < 0 || index >= len {
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response("ERR index out of range").into(),
                    );
                }
                // 将索引转换为usize并替换元素
                let idx = index as usize;
                if let Some(element) = list.get_mut(idx) {
                    *element = self.value;
                } else {
                    // 理论上不会发生，因为我们已经检查了索引范围
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response("ERR index out of range").into(),
                    );
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::SimpleString("OK".to_string()),
                )
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // LSET cannot create a new key
        (
            MochaOperation::Abort,
            ProtocolError::response("ERR no such key").into(),
        )
    }
}
