use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::MultiReadComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::hll::{
    HllDecodeError,
    RedisHll,
};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

const WRONG_HLL_TYPE: &str =
    "WRONGTYPE Key is not a valid HyperLogLog string value.";

const CORRUPTED_HLL: &str =
    "INVALIDOBJ Corrupted HLL object detected";

/// Parameters for PFMERGE.
///
/// Redis syntax:
///
///     PFMERGE destkey [sourcekey [sourcekey ...]]
///
/// 注意：
///
/// `keys` 不只是 source keys。
///
/// 它故意包含：
///
///     [destination, source1, source2, ...]
///
/// 原因是 Redis PFMERGE 会把 destination 当前的 HLL
/// 也一起参与 merge。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PFMergeParams {
    /// Destination / write key.
    pub key: Bytes,

    /// 所有需要读取并参与 merge 的 key。
    ///
    /// keys[0] == key
    pub keys: Vec<Bytes>,
}

impl PFMergeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        /*
         * Redis command arity:
         *
         *     PFMERGE destkey [sourcekey ...]
         *
         * 所以最少只有 command + destination 两项。
         *
         * PFMERGE dest
         *
         * 是合法命令。
         */
        if items.len() < 2 {
            return Err(
                ProtocolError::WrongArgCount("PFMERGE")
            );
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument("key")
            )?;

        /*
         * 非常重要：
         *
         * Redis pfmergeCommand:
         *
         *     for (j = 1; j < c->argc; j++)
         *
         * 从 argv[1] 开始，也就是 destination 本身
         * 也参与 HLL union。
         *
         * 因此这里从 skip(1)，不是 skip(2)。
         */
        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() != items.len() - 1 {
            return Err(
                ProtocolError::InvalidArgument("key")
            );
        }

        Ok(Self {
            key,
            keys,
        })
    }
}

impl Display for PFMergeParams {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "PFMERGE")?;

        /*
         * keys 已经包含 destination，
         * 因此不要再单独打印 self.key，
         * 否则 destination 会出现两次。
         */
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

/// PFMERGE command executor.
pub struct PFMergeCommand;

impl RaftCommand for PFMergeCommand {
    fn raft_request(
        &self,
        items: &[Value],
    ) -> Result<Operation, ProtocolError> {
        let params =
            PFMergeParams::parse(items)?;

        Ok(
            Operation::Base(
                BaseOperation::PFMerge(
                    PFMergeReq {
                        key: params.key,
                        keys: params.keys,
                    }
                )
            )
        )
    }
}

#[async_trait]
impl Command for PFMergeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
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

        let operation =
            self.raft_request(items)?;

        let value =
            server
                .app
                .write(
                    operation,
                    client.db_number,
                )
                .await?;

        Ok(value)
    }
}

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Clone,
)]
pub struct PFMergeReq {
    /// Destination key.
    pub key: Bytes,

    /// destination + all source keys.
    ///
    /// keys[0] == key
    pub keys: Vec<Bytes>,
}

impl Display for PFMergeReq {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "PFMERGE")?;

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

impl MultiReadComputeCommand for PFMergeReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        /*
         * 这里必须包含 destination。
         *
         * 例如：
         *
         *     PFMERGE dst src
         *
         * Redis 实际计算：
         *
         *     dst = union(dst, src)
         *
         * 而不是：
         *
         *     dst = src
         */
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::PFMerge(self)
    }

    fn mutate(
        self,
        read_entries: Vec<
            Option<EntrySnapshot<MyValue>>
        >,
        _write_clock: u64,
    ) -> (
        MochaOperation<MyValue>,
        Value,
    ) {
        /*
         * 正常情况下：
         *
         * read_entries.len() == self.keys.len()
         *
         * 而且：
         *
         * read_entries[0]
         *
         * 就是 destination 当前的值。
         */
        debug_assert_eq!(
            read_entries.len(),
            self.keys.len()
        );

        /*
         * PFMERGE 修改已有 destination 时，
         * Redis 不会像 SET 一样清除 TTL。
         *
         * 它直接修改现有 destination value。
         *
         * 因此：
         *
         * - destination 存在 -> 保留 TTL
         * - destination 不存在 -> Persistent
         *
         * 因为 keys[0] 就是 destination，
         * 我们可以直接从第一个 snapshot 获取 expire。
         */
        let expire = read_entries
            .first()
            .and_then(|entry| entry.as_ref())
            .map(|snapshot| {
                snapshot.get_expire_policy()
            })
            .unwrap_or(
                ExpirePolicy::Persistent
            );

        /*
         * RAW register union。
         *
         * Redis PFMERGE 内部也是：
         *
         *     max[i] =
         *         MAX(max[i], hll[i])
         *
         * 共 16384 registers。
         */
        let mut merged =
            RedisHll::new();

        for (
            index,
            entry,
        ) in read_entries
            .into_iter()
            .enumerate()
        {
            let Some(snapshot) = entry else {
                /*
                 * Redis:
                 *
                 * 不存在的 destination/source
                 * 都视为空 HLL。
                 */
                continue;
            };

            let hll =
                match snapshot.value.data {
                    /*
                     * Redis HyperLogLog 在 Redis
                     * 类型系统中就是 String。
                     */
                    ValueObject::String(raw) => {
                        match RedisHll::decode(
                            raw.as_ref()
                        ) {
                            Ok(hll) => hll,

                            /*
                             * 是 String，
                             * 但是：
                             *
                             * - 没有 HYLL magic
                             * - encoding 非法
                             * - dense 长度不对
                             *
                             * Redis 返回专用 WRONGTYPE。
                             */
                            Err(
                                HllDecodeError::NotHll
                            ) => {
                                return (
                                    MochaOperation::Abort,
                                    invalid_hll(),
                                );
                            }

                            /*
                             * Header 是 HLL，
                             * 但 sparse body 损坏。
                             */
                            Err(
                                HllDecodeError::Corrupted
                            ) => {
                                return (
                                    MochaOperation::Abort,
                                    corrupted_hll(),
                                );
                            }
                        }
                    }

                    /*
                     * 你的 ValueObject::Int
                     * 相当于 Redis String 的
                     * integer encoding。
                     *
                     * Redis isHLLObjectOrReply()
                     * 要求 sdsEncodedObject，
                     * integer encoded string
                     * 因而不是合法 HLL。
                     */
                    ValueObject::Int(_) => {
                        return (
                            MochaOperation::Abort,
                            invalid_hll(),
                        );
                    }

                    /*
                     * List / Hash / Set / ZSet...
                     *
                     * Redis checkType()
                     * 在这里返回普通 WRONGTYPE。
                     */
                    _ => {
                        return (
                            MochaOperation::Abort,
                            CacheCatError::from(
                                ProtocolError::WrongType
                            )
                                .into(),
                        );
                    }
                };

            /*
             * 第 0 项就是 destination。
             *
             * 如果 destination 已存在：
             *
             * 直接以 destination HLL 作为 merged
             * 的初始值。
             *
             * 除了 register 本身，这样还能保留：
             *
             * - destination 的 encoding preference
             * - reserved bytes
             * - 原 cardinality cache 内容
             *
             * 最终我们只把 cache 标记成 invalid。
             *
             * 这比：
             *
             *     RedisHll::new();
             *     merged.merge(&destination);
             *
             * 更接近 Redis 对现有 destination
             * 原地修改的行为。
             */
            if index == 0 {
                merged = hll;
            } else {
                merged.merge(&hll);
            }
        }

        /*
         * Redis PFMERGE 无论实际 register 有没有变化，
         * 都会 invalidate destination 的 cached
         * cardinality。
         *
         * 所以不能像 PFADD 一样：
         *
         *     if !changed {
         *         Abort
         *     }
         *
         * PFMERGE 始终是 write command。
         */
        merged.invalidate_cache();

        let bytes =
            merged.into_bytes();

        (
            MochaOperation::Insert {
                value: MyValue::new(
                    ValueObject::String(bytes)
                ),

                /*
                 * PFMERGE 保留已有 destination TTL。
                 */
                expire,
            },

            /*
             * Redis PFMERGE reply:
             *
             *     +OK
             */
            Value::SimpleString(
                String::from("OK")
            ),
        )
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