//! LREM command implementation
//!
//! LREM key count element
//! Remove the first count occurrences of element from the list stored at key.
//!
//! The count argument influences the operation:
//! - count > 0: Remove elements equal to element moving from head to tail
//! - count < 0: Remove elements equal to element moving from tail to head
//! - count = 0: Remove all elements equal to element
//!
//! Returns:
//! - The number of removed elements
//! - 0 if key does not exist
//! - Error if key exists but is not a list

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
use crate::raft::types::entry::bae_operation::BaseOperation::LRem;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;

/// LREM command handler
pub struct LRemCommand;

impl LRemCommand {
    /// Parse arguments
    /// Format: LREM key count element
    fn parse_args(items: &[Value]) -> Result<LRemArgs, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("lrem"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let count = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("count"))?;
        let element = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("element"))?;
        Ok(LRemArgs {
            key,
            count,
            element,
        })
    }
}

/// Parsed LREM arguments
struct LRemArgs {
    key: Bytes,
    count: i64,
    element: Bytes,
}

impl RaftCommand for LRemCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(LRem(LRemReq {
            key: params.key,
            count: params.count,
            element: params.element,
        })))
    }
}

#[async_trait]
impl Command for LRemCommand {
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
pub struct LRemReq {
    pub key: Bytes,
    pub count: i64,
    pub element: Bytes,
}

impl Display for LRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LRemReq {{ key: {}, count: {}, element: {} }}",
            String::from_utf8_lossy(&self.key),
            self.count,
            String::from_utf8_lossy(&self.element)
        )
    }
}

impl ComputeCommand for LRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let mut list = data_arc.lock();
                let removed_count = self.remove_elements(&mut list);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(removed_count),
                )
            }
            _ => (Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // Key不存在时返回0
        (Abort, Value::Integer(0))
    }
}

impl LRemReq {
    /// Remove elements from the list based on count value
    fn remove_elements(&self, list: &mut VecDeque<Bytes>) -> i64 {
        let mut removed = 0i64;

        match self.count.cmp(&0) {
            std::cmp::Ordering::Greater => {
                // count > 0: 从头到尾移除count个匹配元素
                let mut count = self.count;
                let mut i = 0;
                while i < list.len() && count > 0 {
                    if list[i] == self.element {
                        list.remove(i);
                        removed += 1;
                        count -= 1;
                        // 移除后不需要增加i，因为下一个元素会移到当前位置
                    } else {
                        i += 1;
                    }
                }
            }
            std::cmp::Ordering::Less => {
                // count < 0: 从尾到头移除|count|个匹配元素
                let mut count = -self.count;
                let mut i = list.len();
                while i > 0 && count > 0 {
                    i -= 1;
                    if list[i] == self.element {
                        list.remove(i);
                        removed += 1;
                        count -= 1;
                        // 由于remove会调整索引，但我们已经从后往前遍历，所以不需要特殊处理
                    }
                }
            }
            std::cmp::Ordering::Equal => {
                // count = 0: 移除所有匹配元素
                let mut i = 0;
                while i < list.len() {
                    if list[i] == self.element {
                        list.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            }
        }

        removed
    }
}
