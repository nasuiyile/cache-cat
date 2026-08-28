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
use crate::raft::types::entry::request::Operation;

use crate::raft::types::core::mocha::bloom_filter::{BloomError, BloomObject};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct BfAddParams {
    pub key: Bytes,
    pub item: Bytes,
}

impl BfAddParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("BF.ADD"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let item = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("item"))?;

        Ok(Self { key, item })
    }
}

pub struct BfAddCommand;

impl RaftCommand for BfAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BfAddParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::BfAdd(BfAddReq {
            key: params.key,
            item: params.item,
        })))
    }
}

#[async_trait]
impl Command for BfAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);

            return Ok(Value::SimpleString("QUEUED".to_string()));
        }

        let operation = self.raft_request(items)?;

        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BfAddReq {
    pub key: Bytes,
    pub item: Bytes,
}

impl fmt::Display for BfAddReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BfAddReq {{ key: {}, item: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.item),
        )
    }
}

impl ComputeCommand for BfAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BfAdd(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();

        let add_result = {
            let bloom = match &entry.value.data {
                ValueObject::Bloom(bloom) => bloom,

                _ => {
                    return (MochaOperation::Abort, ProtocolError::WrongType.into());
                }
            };

            let mut bloom = bloom.lock();

            bloom.add(&self.item)
        };

        match add_result {
            Ok(true) => (
                MochaOperation::Insert {
                    value: entry.value,
                    expire,
                },
                Value::Boolean(true),
            ),

            /*
             * Bloom 判断 item 已经存在时
             * 没有发生任何 mutation。
             */
            Ok(false) => (MochaOperation::Abort, Value::Boolean(false)),

            Err(error) => (MochaOperation::Abort, bloom_insert_error(error).into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut bloom = match BloomObject::redis_default() {
            Ok(bloom) => bloom,

            Err(error) => {
                return (MochaOperation::Abort, bloom_create_error(error).into());
            }
        };

        let added = match bloom.add(&self.item) {
            Ok(added) => added,

            Err(error) => {
                return (MochaOperation::Abort, bloom_insert_error(error).into());
            }
        };

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(bloom)))),

                expire: ExpirePolicy::Persistent,
            },
            Value::Boolean(added),
        )
    }
}

#[inline]
fn bloom_insert_error(error: BloomError) -> ProtocolError {
    match error {
        BloomError::Full => ProtocolError::BloomFilterFull,

        BloomError::OutOfMemory | BloomError::Invalid | BloomError::Overflow => {
            ProtocolError::BloomInsertFailed
        }
    }
}

#[inline]
fn bloom_create_error(error: BloomError) -> ProtocolError {
    match error {
        BloomError::OutOfMemory => ProtocolError::BloomCreateOutOfMemory,

        BloomError::Full | BloomError::Invalid | BloomError::Overflow => {
            ProtocolError::BloomCreateFailed
        }
    }
}
