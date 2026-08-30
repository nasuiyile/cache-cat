use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::bloom_filter::{BloomError, BloomObject};
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
        /*
         * 最少：
         *
         * BF.MADD key item
         *
         * values.len() == 3
         */
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
        /*
         * BF.MADD 与 BF.ADD 一样：
         *
         * 修改已有 key 时保留 TTL。
         */
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
        /*
         * key 不存在：
         *
         * Redis BF.MADD 自动创建 Bloom Filter。
         */
        let mut bloom = match BloomObject::redis_default() {
            Ok(bloom) => bloom,

            Err(error) => {
                return (MochaOperation::Abort, bloom_create_error(error).into());
            }
        };

        /*
         * 创建成功后按 Redis 顺序逐个 ADD。
         */
        let (replies, _mutated) = add_items(&mut bloom, &self.items);

        /*
         * 一旦 Bloom filter 已经成功创建，
         * 就应该 Insert。
         *
         * 不要根据 mutated 决定 Abort。
         *
         * Redis bfInsertCommon() 也是先创建 filter，
         * 然后再依次执行 SBChain_Add。
         */
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(bloom)))),

                expire: ExpirePolicy::Persistent,
            },
            Value::Array(Some(replies)),
        )
    }
}

/// 执行 BF.MADD items。
///
/// 必须由调用方保证 BloomObject 已经被独占锁住。
///
/// 返回：
///
/// (每个 item 的 Redis reply, 是否修改过 filter)
fn add_items(bloom: &mut BloomObject, items: &[Bytes]) -> (Vec<Value>, bool) {
    let mut replies = Vec::with_capacity(items.len());

    let mut mutated = false;

    for item in items {
        match bloom.add(item) {
            /*
             * 新元素。
             *
             * RESP3:
             *   true
             *
             * RESP2:
             *   Value encoder 应把 Boolean(true)
             *   编码成 :1
             */
            Ok(true) => {
                mutated = true;

                replies.push(Value::Boolean(true));
            }

            /*
             * Bloom 判断可能已经存在。
             */
            Ok(false) => {
                replies.push(Value::Boolean(false));
            }

            /*
             * RedisBloom 的特殊语义：
             *
             * NONSCALING filter 满以后：
             *
             * 1. 当前位置放一个 error
             * 2. 停止整个 MADD
             * 3. 后续 items 不再执行
             *
             * RedisBloom:
             *
             * for (... && rv != -2)
             */
            Err(BloomError::Full) => {
                replies.push(ProtocolError::BloomFilterFull.into());

                break;
            }

            /*
             * RedisBloom SBChain_Add() == -1：
             *
             * 当前数组元素返回：
             *
             * ERR problem inserting into filter
             *
             * 但是不会停止 BF.MADD，
             * 会继续处理下一个 item。
             */
            Err(BloomError::OutOfMemory | BloomError::Invalid | BloomError::Overflow) => {
                replies.push(ProtocolError::BloomInsertFailed.into());
            }
        }
    }

    (replies, mutated)
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
