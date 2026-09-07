//! HDEL command implementation
//!
//! HDEL key field [field ...]
//! Deletes one or more fields from the hash stored at key.
//! Specified fields that do not exist within this hash are ignored.
//!
//! Returns:
//! - The number of fields that were removed from the hash, not including
//!   specified but non-existing fields
//!
//! Note: This command uses atomic batch write to ensure all field deletions
//! and metadata updates are written together as a single atomic operation.

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
use crate::raft::types::entry::bae_operation::BaseOperation::HDel;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Parsed HDEL arguments
#[derive(Debug)]
struct HDelParam {
    key: Bytes,
    fields: Vec<Bytes>, // fields to delete
}

/// HDEL command handler
pub struct HDelCommand;

impl HDelCommand {
    /// Parse arguments from RESP items
    /// Format: HDEL key field [field ...]
    fn parse_args(items: &[Value]) -> Result<HDelParam, ProtocolError> {
        // Minimum: HDEL key field (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hdel"));
        }
        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        // Parse fields from items[2..]
        let fields = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if fields.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("field"));
        }
        Ok(HDelParam { key, fields })
    }
}

impl RaftCommand for HDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let operation = HDel(HDelReq {
            key: params.key,
            fields: params.fields,
        });
        Ok(Operation::Base(operation))
    }
}

#[async_trait]
impl Command for HDelCommand {
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
pub struct HDelReq {
    pub key: Bytes,
    pub fields: Vec<Bytes>,
}

impl Display for HDelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HDelReq {{ key: {}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.fields
        )
    }
}

impl ComputeCommand for HDelReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HDel(self.clone())
    }
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut map = hash.lock();
                let mut deleted_count = 0;
                for field in &self.fields {
                    if map.remove(field).is_some() {
                        deleted_count += 1;
                    }
                }
                drop(map);
                if deleted_count == 0 {
                    return (MochaOperation::Abort, Value::Integer(0));
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(deleted_count),
                )
            }
            _ => (MochaOperation::Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}
