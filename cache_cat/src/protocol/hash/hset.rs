//! HSET command implementation
//!
//! HSET key field value [field value ...]
//! Sets the specified fields to their respective values in the hash stored at key.
//!
//! Returns:
//! - The number of fields that were added (not updated)
//!
//! Note: This command uses atomic batch write to ensure all fields and metadata
//! are written together as a single atomic operation.

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
use crate::raft::types::entry::bae_operation::BaseOperation::HSet;
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Parsed HSET arguments
#[derive(Debug)]
struct HSetParam {
    key: Bytes,
    fields: Vec<(Bytes, Bytes)>, // (field, value) pairs
}

/// HSET command handler
pub struct HSetCommand;

impl HSetCommand {
    /// Parse arguments from RESP items
    /// Format: HSET key field value [field value ...]
    fn parse_args(items: &[Value]) -> Result<HSetParam, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("hset"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let field_count = items.len() - 2; // items[2] onwards
        if !field_count.is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("hset"));
        }
        let mut fields = Vec::with_capacity(field_count / 2);
        let mut i = 2;
        while i < items.len() {
            let field = items[i]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("field"))?;
            let value = items[i + 1]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("value"))?;
            fields.push((field, value));
            i += 2;
        }
        Ok(HSetParam { key, fields })
    }
}

impl RaftCommand for HSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HSet(HSetReq {
            key: params.key,
            elements: params.fields,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HSetCommand {
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
pub struct HSetReq {
    pub key: Bytes,
    pub elements: Vec<(Bytes, Bytes)>,
}

impl fmt::Display for HSetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HSetReq {{ key: {}, field: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

impl ComputeCommand for HSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HSet(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut count = 0;
                let mut map = hash.lock();
                for (k, v) in &self.elements {
                    let value = parse_i64(v)
                        .map(HashValue::Int)
                        .unwrap_or_else(|| HashValue::Str(v.clone()));
                    if map.insert(k.clone(), value).is_none() {
                        count += 1;
                    }
                }
                drop(map);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(count),
                )
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut map = HashMap::new();
        let len = self.elements.len();
        for (k, v) in self.elements {
            if let Some(int) = parse_i64(&v) {
                map.insert(k.clone(), HashValue::Int(int));
            } else {
                map.insert(k.clone(), HashValue::Str(v.clone()));
            }
        }
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len as i64),
        )
    }
}
