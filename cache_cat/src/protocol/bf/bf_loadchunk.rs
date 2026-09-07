use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::bf::error::{BloomOperation, NOT_FOUND, from_engine};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
#[derive(Debug, Clone, PartialEq)]
pub struct BfLoadChunkParams {
    pub key: Bytes,
    pub iterator: i64,
    pub data: Bytes,
}
impl BfLoadChunkParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("BF.LOADCHUNK"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let iterator = items[2].parse_i64().ok_or(ProtocolError::response(
            "ERR Second argument must be numeric",
        ))?;
        let data = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("data"))?;

        Ok(Self {
            key,
            iterator,
            data,
        })
    }
}

pub struct BfLoadChunkCommand;

impl RaftCommand for BfLoadChunkCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BfLoadChunkParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::BfLoadChunk(
            BfLoadChunkReq {
                key: params.key,
                iterator: params.iterator,
                data: params.data,
            },
        )))
    }
}

#[async_trait]
impl Command for BfLoadChunkCommand {
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
        let operation = self.raft_request(items)?;
        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BfLoadChunkReq {
    pub key: Bytes,
    pub iterator: i64,
    pub data: Bytes,
}

impl fmt::Display for BfLoadChunkReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BfLoadChunkReq {{ key: {}, iterator: {}, data_len: {} }}",
            String::from_utf8_lossy(&self.key),
            self.iterator,
            self.data.len()
        )
    }
}

impl ComputeCommand for BfLoadChunkReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BfLoadChunk(self)
    }
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let result = {
            let bloom = match &entry.value.data {
                ValueObject::Bloom(bloom) => bloom,
                _ => {
                    return (MochaOperation::Abort, ProtocolError::WrongType.into());
                }
            };
            let mut bloom = bloom.lock();
            bloom.load_dump_chunk(self.iterator, &self.data)
        };
        match result {
            Ok(()) => (
                MochaOperation::Insert {
                    value: entry.value,
                    expire,
                },
                Value::ok(),
            ),
            Err(error) => (
                MochaOperation::Abort,
                from_engine(error, BloomOperation::LoadChunk).into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        if self.iterator != 1 {
            return (
                MochaOperation::Abort,
                ProtocolError::response(NOT_FOUND).into(),
            );
        }
        let bloom = match BloomObject::from_dump_header(&self.data) {
            Ok(bloom) => bloom,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    from_engine(error, BloomOperation::LoadChunk).into(),
                );
            }
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(bloom)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::ok(),
        )
    }
}
