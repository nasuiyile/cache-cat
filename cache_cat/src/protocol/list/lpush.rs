//! LPUSH command implementation
//!
//! LPUSH key element [element ...]
//! Insert all the specified values at the head of the list stored at key.
//! If key does not exist, it is created as empty list before performing the push.
//! When key holds a value that is not a list, an error is returned.
//!
//! Returns:
//! - The length of the list after the push operations (integer reply)
//!
//! Note: Elements are inserted one after the other from leftmost to rightmost.
//! `LPUSH mylist a b c` results in `[c, b, a]`.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::LPush;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::lock_api::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

/// LPUSH command handler
pub struct LPushCommand;

impl LPushCommand {
    /// Parse arguments from RESP items
    /// Format: LPUSH key element [element ...]
    fn parse_args(items: &[Value]) -> Result<LPushArgs, ProtocolError> {
        // Minimum: LPUSH key element (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("lpush"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let elements = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();
        if elements.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("element"));
        }
        Ok(LPushArgs { key, elements })
    }
}

/// Parsed LPUSH arguments
struct LPushArgs {
    key: Bytes,
    elements: Vec<Bytes>,
}

impl RaftCommand for LPushCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(LPush(LPushReq {
            key: params.key,
            elements: params.elements,
        })))
    }
}

#[async_trait]
impl Command for LPushCommand {
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
pub struct LPushReq {
    pub key: Bytes,
    pub elements: Vec<Bytes>,
}

impl Display for LPushReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LPushReq {{ key: {}, elements: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

impl ComputeCommand for LPushReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LPush(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let len = {
                    let mut list = data_arc.lock();
                    for element in self.elements {
                        list.push_front(element);
                    }
                    list.len() as i64
                };
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(len),
                )
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let deque: VecDeque<_> = VecDeque::from(self.elements);
        let len = deque.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(deque)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}
