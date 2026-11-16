//! JSON.DEL key [path]
//!
//! Deletes every value matched by `path` and returns how many were deleted.
//! An omitted path is the root, and deleting the root deletes the key.

use super::path::JsonPath;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonDelReq {
    pub key: Bytes,
    pub path: String,
}

impl fmt::Display for JsonDelReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.DEL {} {}",
            String::from_utf8_lossy(&self.key),
            self.path
        )
    }
}

impl JsonDelReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("json.del"));
        }
        // A partial deletion stores the document again under this key, which
        // must not keep the shared receive buffer alive.
        let key = Bytes::copy_from_slice(&super::get::bytes_arg(&items[1])?);
        // Redis reads a single optional path and ignores further arguments.
        let path = match items.get(2) {
            Some(path) => super::get::text_arg(path)?,
            None => "$".to_owned(),
        };
        Ok(Self { key, path })
    }
}

impl ComputeCommand for JsonDelReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::JsonDel(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let ValueObject::Json(document) = &entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let path = match JsonPath::parse(&self.path) {
            Ok(path) => path,
            Err(err) => return (MochaOperation::Abort, Value::error(format!("ERR {err}"))),
        };
        if path.is_root() {
            return (MochaOperation::Remove, Value::Integer(1));
        }
        let mut document = document.lock();
        let deleted = match path.delete(document.value_mut()) {
            Ok(deleted) => deleted,
            Err(err) => return (MochaOperation::Abort, Value::error(format!("ERR {err}"))),
        };
        // Redis also deletes a key whose root container this command emptied.
        let emptied = document.value().is_empty() == Some(true);
        drop(document);
        if deleted == 0 {
            return (MochaOperation::Abort, Value::Integer(0));
        }
        let response = Value::Integer(deleted as i64);
        if emptied {
            return (MochaOperation::Remove, response);
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: entry.get_expire_policy(),
            },
            response,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // Redis does not compile the path of a missing key.
        (MochaOperation::Abort, Value::Integer(0))
    }
}

pub struct JsonDelCommand;

impl RaftCommand for JsonDelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::JsonDel(JsonDelReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for JsonDelCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let operation = self.raft_request(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(operation);
            return Ok(Value::queued());
        }
        server.app.write(operation, client.db_number).await
    }
}
