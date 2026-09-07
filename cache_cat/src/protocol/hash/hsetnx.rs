//! HSETNX command implementation
//!
//! HSETNX key field value
//! Sets field in the hash stored at key to value, only if field does not yet exist.
//! If key does not exist, a new key holding a hash is created.
//! If field already exists, this operation has no effect.
//!
//! Returns:
//! - 1 if field is a new field in the hash and value was set
//! - 0 if field already exists in the hash and no operation was performed
//!
//! Note: This command uses atomic batch write to ensure all fields and metadata
//! are written together as a single atomic operation.

use std::collections::HashMap;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::HSetNx;
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Parsed HSETNX arguments
#[derive(Debug)]
struct HSetNxParam {
    key: Bytes,
    field: Bytes,
    value: Bytes,
}

/// HSETNX command handler
pub struct HSetNxCommand;

impl HSetNxCommand {
    /// Parse arguments from RESP items
    /// Format: HSETNX key field value
    fn parse_args(items: &[Value]) -> Result<HSetNxParam, ProtocolError> {
        // HSETNX requires exactly 4 items: command + key + field + value
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("hsetnx"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let field = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("field"))?;
        let value = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;
        Ok(HSetNxParam { key, field, value })
    }
}

impl RaftCommand for HSetNxCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HSetNx(HSetNxReq {
            key: params.key,
            field: params.field,
            value: params.value,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HSetNxCommand {
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
pub struct HSetNxReq {
    pub key: Bytes,
    pub field: Bytes,
    pub value: Bytes,
}

impl fmt::Display for HSetNxReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HSetNxReq {{ key: {}, field: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field),
            String::from_utf8_lossy(&self.value),
        )
    }
}

impl ComputeCommand for HSetNxReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HSetNx(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut map = hash.lock();
                if map.contains_key(&self.field) {
                    // Field exists, no operation performed
                    drop(map);
                    (
                        MochaOperation::Abort,
                        Value::Integer(0),
                    )
                } else {
                    // Field doesn't exist, set it
                    let value = parse_i64(&self.value)
                        .map(HashValue::Int)
                        .unwrap_or_else(|| HashValue::Str(self.value));

                    map.insert(self.field, value);
                    drop(map);
                    (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::Integer(1),
                    )
                }
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut map = HashMap::new();
        let value = if let Some(int) = parse_i64(&self.value) {
            HashValue::Int(int)
        } else {
            HashValue::Str(self.value)
        };
        map.insert(self.field, value);
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(1), // Always returns 1 for new key
        )
    }
}
