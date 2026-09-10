//! XADD command implementation.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{AddId, Fields, RedisStream};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Parsed parameters for the basic XADD form.
///
/// `XADD key id field value [field value ...]`
#[derive(Debug, Clone, PartialEq)]
pub struct XAddParams {
    /// Key that holds the stream.
    pub key: Bytes,
    /// Parsed entry ID, for example `*` or `1234567890-0`.
    pub id: AddId,
    /// Ordered, binary-safe field-value pairs.
    pub fields: Fields,
}

impl XAddParams {
    /// Parse the required XADD arguments.
    ///
    /// Stream trimming and `NOMKSTREAM` options will be added together with
    /// the storage execution path.
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: XADD key id field value
        if items.len() < 5 || !(items.len() - 3).is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("xadd"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let id = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("id"))?
            .parse()
            .map_err(|_| ProtocolError::InvalidArgument("id"))?;

        let fields = items[3..]
            .chunks_exact(2)
            .map(|pair| {
                let field = pair[0]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("field"))?;
                let value = pair[1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("value"))?;
                Ok((field.to_vec(), value.to_vec()))
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?;

        Ok(Self { key, id, fields })
    }
}

/// XADD command handler.
pub struct XAddCommand;

impl RaftCommand for XAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = XAddParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::XAdd(XAddReq {
            key: params.key,
            id: params.id,
            fields: params.fields,
        })))
    }
}

#[async_trait]
impl Command for XAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        server
            .app
            .write(self.raft_request(items)?, client.db_number)
            .await
    }
}

/// Replicated XADD write request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAddReq {
    pub key: Bytes,
    pub id: AddId,
    pub fields: Fields,
}

impl fmt::Display for XAddReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XAddReq {{ key: {}, id: {:?}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.id,
            self.fields,
        )
    }
}

impl ComputeCommand for XAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XAdd(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let version = entry.value.version;
        let ValueObject::Stream(mut stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        //使用write_clock 确保所有节点确定性的执行。
        let add_id = match self.id {
            AddId::Auto => AddId::AutoSequence(write_clock),
            _ => self.id,
        };
        let id = match stream.write().xadd(add_id, self.fields) {
            Ok(id) => id,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(format!("ERR {error}")).into(),
                );
            }
        };

        (
            MochaOperation::Insert {
                value: MyValue {
                    version,
                    data: ValueObject::Stream(stream),
                },
                expire,
            },
            Value::BulkString(Some(id.to_string().into())),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut stream = RedisStream::new();
        let id = match stream.xadd(self.id, self.fields) {
            Ok(id) => id,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(format!("ERR {error}")).into(),
                );
            }
        };
        let value = MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(stream))));
        (
            MochaOperation::Insert {
                value,
                expire: ExpirePolicy::Persistent,
            },
            Value::BulkString(Some(id.to_string().into())),
        )
    }
}
