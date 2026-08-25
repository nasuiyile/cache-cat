use crate::error::{
    CacheCatError,
    ProtocolError,
};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{
    Client,
    Command,
};
use crate::protocol::raft_command::{
    RaftCommand,
    ReadRaftCommand,
};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::hll::{
    HllDecodeError,
    RedisHll,
};
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{
    Deserialize,
    Serialize,
};
use std::fmt::Display;

const WRONG_HLL_TYPE: &str =
    "WRONGTYPE Key is not a valid HyperLogLog string value.";

const CORRUPTED_HLL: &str =
    "INVALIDOBJ Corrupted HLL object detected";

/// Parameters for PFCOUNT command.
///
/// Redis:
///
/// PFCOUNT key [key ...]
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
)]
pub struct PfcountParams {
    pub keys: Vec<Bytes>,
}

impl Display for PfcountParams {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "PFCOUNT")?;

        for key in &self.keys {
            write!(
                f,
                " {}",
                String::from_utf8_lossy(key)
            )?;
        }

        Ok(())
    }
}

impl PfcountParams {
    fn parse(
        items: &[Value],
    ) -> Result<Self, ProtocolError> {
        /*
         * Redis PFCOUNT arity = -2
         *
         * 所以：
         *
         * PFCOUNT
         *
         * 非法；
         *
         * PFCOUNT key
         *
         * 合法。
         */
        if items.len() < 2 {
            return Err(
                ProtocolError::WrongArgCount(
                    "PFCOUNT"
                )
            );
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(
                Value::string_bytes_clone
            )
            .collect::<Vec<_>>();

        if keys.len() != items.len() - 1 {
            return Err(
                ProtocolError::InvalidArgument(
                    "key"
                )
            );
        }

        Ok(Self { keys })
    }
}

/// PFCOUNT command executor.
pub struct PfcountCommand;

impl ReadRaftCommand for PfcountCommand {
    fn read_operation(
        &self,
        items: &[Value],
    ) -> Result<ReadOperation, ProtocolError> {
        Ok(
            ReadOperation::PFCount(
                PfcountParams::parse(items)?
            )
        )
    }
}

#[async_trait]
impl Command for PfcountCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        /*
         * 和你的 MGET 完全相同：
         *
         * MULTI
         * PFCOUNT ...
         *
         * => QUEUED
         */
        if let Some(queue) =
            client.transaction_queue.as_mut()
        {
            queue.push(
                self.raft_request(items)?
            );

            return Ok(
                Value::SimpleString(
                    String::from("QUEUED")
                )
            );
        }

        let params =
            self.read_operation(items)?;

        server
            .app
            .multi_read(
                params,
                client.db_number,
            )
            .await
    }
}

impl MultiReadCommand for PfcountParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(
        &self,
        values: Vec<
            Option<EntrySnapshot<MyValue>>
        >,
    ) -> Value {
        /*
         * PFCOUNT 只有一个 key 时，
         * Redis 有特殊优化：
         *
         * 如果 cached cardinality 有效，
         * 直接返回 cached value。
         *
         * 我们可以安全地读取这个 cache，
         * 但不在 read command 中回写 cache。
         */
        if self.keys.len() == 1 {
            return pfcount_single(
                values.into_iter().next().flatten()
            );
        }

        /*
         * PFCOUNT key1 key2 ...
         *
         * Redis 创建临时 RAW HLL：
         *
         *     union[i] =
         *         max(key1[i], key2[i], ...)
         *
         * RedisHll::new() 的所有 register 都是 0，
         * 正好等价于 Redis HLL_RAW 的初始化状态。
         */
        let mut union =
            RedisHll::new();

        for value in values {
            let Some(snapshot) = value else {
                /*
                 * 不存在的 key：
                 *
                 * Redis 将其视为空 HLL。
                 */
                continue;
            };

            match snapshot.value.data {
                /*
                 * HyperLogLog 在 Redis 类型系统中
                 * 就是 String。
                 */
                ValueObject::String(raw) => {
                    let hll =
                        match RedisHll::decode(
                            raw.as_ref()
                        ) {
                            Ok(hll) => hll,

                            /*
                             * String，
                             * 但不是合法 HLL。
                             */
                            Err(
                                HllDecodeError::NotHll
                            ) => {
                                return invalid_hll();
                            }

                            /*
                             * HLL header 看起来正确，
                             * 但是 sparse body 损坏。
                             */
                            Err(
                                HllDecodeError::Corrupted
                            ) => {
                                return corrupted_hll();
                            }
                        };

                    union.merge(&hll);
                }

                /*
                 * 你的 Int 本质上相当于 Redis
                 * String 的 integer encoding。
                 *
                 * Redis HLL 要求 SDS encoded string，
                 * integer encoded string 不会被认为
                 * 是合法 HLL。
                 *
                 * 所以这里应该返回：
                 *
                 * WRONGTYPE Key is not a valid
                 * HyperLogLog string value.
                 */
                ValueObject::Int(_) => {
                    return invalid_hll();
                }

                /*
                 * List / Hash / Set / ZSet
                 *
                 * Redis checkType() 会返回普通
                 * WRONGTYPE。
                 */
                _ => {
                    return ProtocolError::WrongType
                        .into();
                }
            }
        }

        Value::Integer(
            union.cardinality() as i64
        )
    }
}

/// 单 key PFCOUNT。
fn pfcount_single(
    value: Option<EntrySnapshot<MyValue>>,
) -> Value {
    let Some(snapshot) = value else {
        /*
         * PFCOUNT nonexistent
         *
         * Redis:
         *
         * => 0
         */
        return Value::Integer(0);
    };

    match snapshot.value.data {
        ValueObject::String(raw) => {
            /*
             * Redis 单 key PFCOUNT 首先进行
             * HLL header/type validation。
             *
             * 如果 cached cardinality 有效，
             * 不需要扫描 16384 个 register。
             */
            match RedisHll::cached_cardinality(
                raw.as_ref()
            ) {
                Ok(Some(cardinality)) => {
                    return Value::Integer(
                        cardinality as i64
                    );
                }

                /*
                 * Cache invalid：
                 *
                 * 继续真正解析 HLL，
                 * 然后重新计算。
                 *
                 * 但由于这里是你的 ReadCommand，
                 * 我们不回写 cache。
                 */
                Ok(None) => {}

                Err(HllDecodeError::NotHll) => {
                    return invalid_hll();
                }

                /*
                 * cached_cardinality 当前实际上
                 * 只进行 header validation，
                 * 正常不会返回 Corrupted。
                 *
                 * 保留这个分支防止以后实现变化。
                 */
                Err(HllDecodeError::Corrupted) => {
                    return corrupted_hll();
                }
            }

            let hll =
                match RedisHll::decode(
                    raw.as_ref()
                ) {
                    Ok(hll) => hll,

                    Err(
                        HllDecodeError::NotHll
                    ) => {
                        return invalid_hll();
                    }

                    Err(
                        HllDecodeError::Corrupted
                    ) => {
                        return corrupted_hll();
                    }
                };

            Value::Integer(
                hll.cardinality() as i64
            )
        }

        ValueObject::Int(_) => {
            invalid_hll()
        }

        _ => {
            ProtocolError::WrongType.into()
        }
    }
}

#[inline]
fn invalid_hll() -> Value {
    Value::Error(
        WRONG_HLL_TYPE.to_string()
    )
}

#[inline]
fn corrupted_hll() -> Value {
    Value::Error(
        CORRUPTED_HLL.to_string()
    )
}