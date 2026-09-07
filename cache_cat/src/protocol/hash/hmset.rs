//! HMSET command implementation
//!
//! HMSET key field value [field value ...]
//! Sets multiple hash fields to multiple values.

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
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Parsed HMSET arguments
#[derive(Debug)]
struct HMSetParams {
    key: Bytes,
    fields: Vec<(Bytes, Bytes)>,
}

/// HMSET command handler
pub struct HMSetCommand;

impl HMSetCommand {
    /// Parse arguments from RESP items
    ///
    /// Format:
    /// HMSET key field value [field value ...]
    fn parse_args(items: &[Value]) -> Result<HMSetParams, ProtocolError> {
        if items.len() < 4 || items.len() % 2 != 0 {
            return Err(ProtocolError::WrongArgCount("hmset"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let mut fields = Vec::with_capacity((items.len() - 2) / 2);
        let mut index = 2;
        while index < items.len() {
            let field = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("field"))?;
            let value = items[index + 1]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("value"))?;
            fields.push((field, value));
            index += 2;
        }
        Ok(HMSetParams { key, fields })
    }
}

impl RaftCommand for HMSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        let operation = BaseOperation::HMSet(HMSetReq {
            key: params.key,
            fields: params.fields,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HMSetCommand {
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
pub struct HMSetReq {
    pub key: Bytes,
    pub fields: Vec<(Bytes, Bytes)>,
}

impl fmt::Display for HMSetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HMSetReq {{ key: {}, fields: {} }}",
            String::from_utf8_lossy(&self.key),
            self.fields.len()
        )
    }
}

impl ComputeCommand for HMSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HMSet(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut map = hash.lock();
                for (field, value) in self.fields {
                    map.insert(field, HashValue::Str(value));
                }
                drop(map);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        // HMSET 不应该改变已有 key 的 TTL
                        expire: entry.get_expire_policy(),
                    },
                    Value::SimpleString(String::from("OK")),
                )
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut map = HashMap::with_capacity(self.fields.len());
        for (field, value) in self.fields {
            map.insert(field, HashValue::Str(value));
        }
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::SimpleString(String::from("OK")),
        )
    }
}
