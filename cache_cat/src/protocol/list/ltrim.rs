//! LTRIM command implementation
//!
//! LTRIM key start stop
//! Trim an existing list so that it will contain only the specified range of elements.
//! Both start and stop are zero-based indexes, where 0 is the first element of the list
//! (the head), 1 the next element and so on.
//!
//! Negative numbers can be used to designate indexes starting at the tail of the list.
//! Here, -1 is the last element, -2 the penultimate element and so on.
//!
//! Out of range indexes will not produce an error:
//! - If start is larger than the end of the list, the list will become empty
//! - If stop is larger than the end of the list, Redis will treat it like the last element
//!
//! Returns:
//! - Simple string "OK"

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
use crate::raft::types::entry::bae_operation::BaseOperation::LTrim;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// LTRIM command handler
pub struct LTrimCommand;

impl LTrimCommand {
    /// Parse arguments
    /// Format: LTRIM key start stop
    fn parse_args(items: &[Value]) -> Result<LTrimArgs, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("ltrim"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let start = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("start"))?;
        let stop = items[3]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("stop"))?;
        Ok(LTrimArgs { key, start, stop })
    }
}

/// Parsed LTRIM arguments
struct LTrimArgs {
    key: Bytes,
    start: i64,
    stop: i64,
}

impl RaftCommand for LTrimCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(LTrim(LTrimReq {
            key: params.key,
            start: params.start,
            stop: params.stop,
        })))
    }
}

#[async_trait]
impl Command for LTrimCommand {
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
pub struct LTrimReq {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
}

impl Display for LTrimReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LTrimReq {{ key: {}, start: {}, stop: {} }}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.stop
        )
    }
}

impl ComputeCommand for LTrimReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LTrim(self.clone())
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
                if len == 0 {
                    return (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::SimpleString(String::from("OK")),
                    );
                }
                // Convert negative indexes to positive
                let mut start = self.start;
                let mut stop = self.stop;

                if start < 0 {
                    start = len + start;
                }
                if stop < 0 {
                    stop = len + stop;
                }
                // Clamp start to [0, len)
                if start < 0 {
                    start = 0;
                }
                // Clamp stop to [-1, len)
                if stop >= len {
                    stop = len - 1;
                }
                // If start > stop, the list becomes empty
                if start > stop {
                    list.clear();
                } else {
                    // Keep only elements in range [start, stop]
                    let start = start as usize;
                    let stop = stop as usize;
                    // Remove elements from the end first to avoid index shifting issues
                    let _new_len = stop - start + 1;
                    list.truncate(stop + 1); // Remove elements after stop

                    // Remove elements before start
                    if start > 0 {
                        // Drain elements from 0 to start
                        list.drain(0..start);
                    }
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::SimpleString(String::from("OK")),
                )
            }
            _ => (
                Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (Abort, Value::SimpleString(String::from("OK")))
    }
}
