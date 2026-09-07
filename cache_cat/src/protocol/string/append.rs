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
use crate::raft::types::entry::bae_operation::BaseOperation::Append;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Parameters for APPEND command
#[derive(Debug, Clone, PartialEq)]
pub struct AppendParams {
    pub key: Bytes,
    pub value: Bytes,
}

impl AppendParams {
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("APPEND"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let value = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(AppendParams::new(key, value))
    }
}

impl RaftCommand for AppendCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = AppendParams::parse(items)?;

        Ok(Operation::Base(Append(AppendReq {
            key: params.key,
            value: params.value,
        })))
    }
}

/// APPEND command executor
pub struct AppendCommand;

#[async_trait]
impl Command for AppendCommand {
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
pub struct AppendReq {
    pub key: Bytes,
    pub value: Bytes,
}

impl Display for AppendReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AppendReq {{ key: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

impl ComputeCommand for AppendReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Append(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::String(data_arc) => {
                // Construct a new string: original content + appended content
                let mut new_buf = BytesMut::from(data_arc.clone());
                new_buf.extend_from_slice(&self.value);
                let len = new_buf.len() as i64;
                let new_value = MyValue::new(ValueObject::String(new_buf.freeze()));
                (
                    MochaOperation::Insert {
                        value: new_value,
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
        let len = self.value.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(self.value)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}
