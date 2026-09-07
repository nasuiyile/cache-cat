use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::bf::error::{BloomOperation, from_engine};
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

/// BF.MADD key item [item ...]
#[derive(Debug, Clone, PartialEq)]
pub struct BfMAddParams {
    pub key: Bytes,
    pub items: Vec<Bytes>,
}

impl BfMAddParams {
    fn parse(values: &[Value]) -> Result<Self, ProtocolError> {
        if values.len() < 3 {
            return Err(ProtocolError::WrongArgCount("BF.MADD"));
        }
        let key = values[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let mut items = Vec::with_capacity(values.len() - 2);
        for value in &values[2..] {
            let item = value
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("item"))?;

            items.push(item);
        }
        Ok(Self { key, items })
    }
}

/// BF.MADD command executor.
pub struct BfMAddCommand;

impl RaftCommand for BfMAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BfMAddParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::BfMAdd(BfMAddReq {
            key: params.key,
            items: params.items,
        })))
    }
}

#[async_trait]
impl Command for BfMAddCommand {
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
pub struct BfMAddReq {
    pub key: Bytes,

    pub items: Vec<Bytes>,
}

impl fmt::Display for BfMAddReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BfMAddReq {{ key: {}, items: {} }}",
            String::from_utf8_lossy(&self.key),
            self.items.len(),
        )
    }
}

impl ComputeCommand for BfMAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BfMAdd(self)
    }
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let (replies, mutated) = {
            let bloom = match &entry.value.data {
                ValueObject::Bloom(bloom) => bloom,
                /*
                 * WRONGTYPE 是 command-level error，
                 * 不是数组中的某一个元素。
                 */
                _ => {
                    return (MochaOperation::Abort, ProtocolError::WrongType.into());
                }
            };
            let mut bloom = bloom.lock();
            add_items(&mut bloom, &self.items)
        };
        let reply = Value::Array(Some(replies));
        if mutated {
            (
                MochaOperation::Insert {
                    value: entry.value,
                    expire,
                },
                reply,
            )
        } else {
            (MochaOperation::Abort, reply)
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut bloom = match BloomObject::redis_default() {
            Ok(bloom) => bloom,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    from_engine(error, BloomOperation::Create).into(),
                );
            }
        };
        let (replies, _mutated) = add_items(&mut bloom, &self.items);
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(bloom)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Array(Some(replies)),
        )
    }
}

fn add_items(bloom: &mut BloomObject, items: &[Bytes]) -> (Vec<Value>, bool) {
    let mut replies = Vec::with_capacity(items.len());
    let mut mutated = false;
    for item in items {
        match bloom.add(item) {
            Ok(true) => {
                mutated = true;
                replies.push(Value::Boolean(true));
            }
            Ok(false) => {
                replies.push(Value::Boolean(false));
            }
            Err(error) => {
                let is_full = matches!(
                    error,
                    crate::raft::types::core::mocha::bloom_filter::BloomError::Full
                );
                replies.push(from_engine(error, BloomOperation::Insert).into());
                if is_full {
                    break;
                }
            }
        }
    }
    (replies, mutated)
}
