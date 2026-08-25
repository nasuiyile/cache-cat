use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{
    EntrySnapshot,
    ExpirePolicy,
    MochaOperation,
};
use crate::protocol::command::{
    Client,
    Command,
};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::hll::{
    HllDecodeError,
    RedisHll,
};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::PfAdd;
use crate::raft::types::entry::request::Operation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{
    Deserialize,
    Serialize,
};
use std::fmt;

const WRONG_HLL_TYPE: &str =
    "WRONGTYPE Key is not a valid HyperLogLog string value.";

const WRONG_VALUE_TYPE: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";

const CORRUPTED_HLL: &str =
    "INVALIDOBJ Corrupted HLL object detected";

/// PFADD key [element [element ...]]
#[derive(Debug, Clone, PartialEq)]
pub struct PfAddParams {
    pub key: Bytes,
    pub elements: Vec<Bytes>,
}

impl PfAddParams {
    fn parse(
        items: &[Value],
    ) -> Result<Self, ProtocolError> {
        // 注意：
        //
        // Redis PFADD 的 arity 是 -2，
        // 所以 PFADD key 是合法的。
        if items.len() < 2 {
            return Err(
                ProtocolError::WrongArgCount("PFADD")
            );
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument("key")
            )?;

        let mut elements =
            Vec::with_capacity(items.len().saturating_sub(2));

        for item in &items[2..] {
            let element = item
                .string_bytes_clone()
                .ok_or(
                    ProtocolError::InvalidArgument("element")
                )?;

            elements.push(element);
        }

        Ok(Self {
            key,
            elements,
        })
    }
}

/// PFADD command executor.
pub struct PfAddCommand;

impl RaftCommand for PfAddCommand {
    fn raft_request(
        &self,
        items: &[Value],
    ) -> Result<Operation, ProtocolError> {
        let params =
            PfAddParams::parse(items)?;

        Ok(
            Operation::Base(
                PfAdd(
                    PfAddReq {
                        key: params.key,
                        elements: params.elements,
                    }
                )
            )
        )
    }
}

#[async_trait]
impl Command for PfAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let operation =
            self.raft_request(items)?;

        if let Some(queue) =
            client.transaction_queue.as_mut()
        {
            queue.push(operation);

            return Ok(
                Value::SimpleString(
                    "QUEUED".to_string()
                )
            );
        }

        server
            .app
            .write(
                operation,
                client.db_number,
            )
            .await
    }
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
)]
pub struct PfAddReq {
    pub key: Bytes,

    /// PFADD element 是 binary-safe 的。
    pub elements: Vec<Bytes>,
}

impl fmt::Display for PfAddReq {
    fn fmt(
        &self,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "PfAddReq {{ key: {}, elements: {} }}",
            String::from_utf8_lossy(&self.key),
            self.elements.len(),
        )
    }
}

impl ComputeCommand for PfAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::PfAdd(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            /*
             * Redis HyperLogLog 属于 String。
             *
             * Bytes 可能来自：
             *
             * PFADD
             * GET/SET
             * RDB
             * replication
             *
             * 因此必须按 Redis HLL binary format 校验。
             */
            ValueObject::String(raw) => {
                /*
                 * 先只检查 header。
                 *
                 * 这一步不能直接 RedisHll::decode()，
                 * 因为 Redis 允许：
                 *
                 * PFADD key
                 *
                 * 没有任何 element。
                 *
                 * 此时 Redis 只验证它是不是 HLL object，
                 * 不执行 hllAdd。
                 */
                if RedisHll::validate_header(raw.as_ref())
                    .is_err()
                {
                    return (
                        MochaOperation::Abort,
                        Value::Error(
                            WRONG_HLL_TYPE.to_string()
                        ),
                    );
                }

                /*
                 * 已存在 key：
                 *
                 * PFADD key
                 *
                 * Redis 返回 0。
                 */
                if self.elements.is_empty() {
                    return (
                        MochaOperation::Abort,
                        Value::Integer(0),
                    );
                }

                let mut hll =
                    match RedisHll::decode(raw.as_ref()) {
                        Ok(hll) => hll,

                        Err(HllDecodeError::NotHll) => {
                            return (
                                MochaOperation::Abort,
                                Value::Error(
                                    WRONG_HLL_TYPE.to_string()
                                ),
                            );
                        }

                        Err(HllDecodeError::Corrupted) => {
                            return (
                                MochaOperation::Abort,
                                Value::Error(
                                    CORRUPTED_HLL.to_string()
                                ),
                            );
                        }
                    };

                let mut updated =
                    false;

                for element in &self.elements {
                    if hll.add(element.as_ref()) {
                        updated = true;
                    }
                }

                /*
                 * Redis PFADD：
                 *
                 * 只要所有元素都没让任何 register 增大，
                 * 返回 0。
                 *
                 * 同时不能重新 Insert，否则可能产生：
                 *
                 * - WATCH/version 变化
                 * - WAL entry
                 * - replication mutation
                 *
                 * 这会偏离 Redis。
                 */
                if !updated {
                    return (
                        MochaOperation::Abort,
                        Value::Integer(0),
                    );
                }

                hll.invalidate_cache();

                let bytes =
                    hll.into_bytes();

                (
                    MochaOperation::Insert {
                        value: MyValue::new(
                            ValueObject::String(bytes)
                        ),

                        // PFADD 不应该清除 TTL。
                        expire:
                        entry.get_expire_policy(),
                    },

                    Value::Integer(1),
                )
            }

            /*
             * 你的 Int 实际类似 Redis String 的 integer encoding。
             *
             * Redis 的 HLL 要求必须是一个合法的 HLL string，
             * 所以这里不是普通 collection WRONGTYPE，
             * 而是 "not a valid HyperLogLog string value"。
             */
            ValueObject::Int(_) => (
                MochaOperation::Abort,

                Value::Error(
                    WRONG_HLL_TYPE.to_string()
                ),
            ),

            /*
             * List / Hash / Set / ZSet
             */
            _ => (
                MochaOperation::Abort,

                Value::Error(
                    WRONG_VALUE_TYPE.to_string()
                ),
            ),
        }
    }

    fn init(
        self,
    ) -> (MochaOperation<MyValue>, Value) {
        let mut hll =
            RedisHll::new();

        /*
         * PFADD key
         *
         * 没有 element 也需要创建一个空 HLL。
         */
        for element in &self.elements {
            hll.add(element.as_ref());
        }

        /*
         * Redis 创建新 PFADD key 时 updated 本身就是 true，
         * 最后同样会 invalid cached cardinality。
         */
        hll.invalidate_cache();

        let bytes =
            hll.into_bytes();

        (
            MochaOperation::Insert {
                value: MyValue::new(
                    ValueObject::String(bytes)
                ),

                expire:
                ExpirePolicy::Persistent,
            },

            /*
             * 非存在 key：
             *
             * PFADD key
             * PFADD key a
             *
             * 都返回 1。
             */
            Value::Integer(1),
        )
    }
}