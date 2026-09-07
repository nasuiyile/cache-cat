use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::SPop;
use crate::raft::types::entry::request::Operation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// SPOP 命令的解析结果。
///
/// count:
/// - None: SPOP key
/// - Some(n): SPOP key count
#[derive(Debug)]
struct SPopArgs {
    key: Bytes,
    count: Option<u64>,
}

pub struct SPopCommand;

impl SPopCommand {
    fn parse_args(items: &[Value]) -> Result<SPopArgs, ProtocolError> {
        // SPOP key
        // SPOP key count
        if items.len() != 2 && items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("spop"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let count = if items.len() == 3 {
            Some(Self::parse_count(&items[2])?)
        } else {
            None
        };

        Ok(SPopArgs { key, count })
    }

    fn parse_count(value: &Value) -> Result<u64, ProtocolError> {
        /*
         * Redis 命令参数通常以 BulkString 进入。
         *
         * 这里先获取参数字节，再按有符号 64 位整数解析，
         * 从而正确拒绝：
         *
         * - 非整数
         * - 溢出 i64 的整数
         * - 负数
         */
        let bytes = value
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("count"))?;

        let text = std::str::from_utf8(bytes.as_ref())
            .map_err(|_| ProtocolError::InvalidArgument("count"))?;

        let count = text
            .parse::<i64>()
            .map_err(|_| ProtocolError::InvalidArgument("count"))?;

        if count < 0 {
            return Err(ProtocolError::InvalidArgument("count"));
        }

        Ok(count as u64)
    }
}

impl RaftCommand for SPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(SPop(SPopReq {
            key: params.key,
            count: params.count,
        })))
    }
}

#[async_trait]
impl Command for SPopCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // MULTI/EXEC 事务模式
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);

            return Ok(Value::queued());
        }

        let operation = self.raft_request(items)?;

        let value = server
            .app
            .write(operation, client.db_number)
            .await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SPopReq {
    pub key: Bytes,

    /// None 表示没有传 count，响应必须是 BulkString。
    ///
    /// Some(count) 表示传了 count，响应必须是 Array，
    /// 即使 count 为 1，也仍然返回数组。
    pub count: Option<u64>,
}

impl Display for SPopReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SPopReq {{ key: {}, count: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.count
        )
    }
}

/// 一个简单、确定性的伪随机数生成器。
///
/// 不能在 Raft 状态机的 mutate() 中使用 thread_rng()，因为每个节点
/// 会生成不同的随机数，最终导致状态机数据不一致。
///
/// 这里采用 SplitMix64：
///
/// 1. 算法简单；
/// 2. 不依赖额外 crate；
/// 3. 给定相同 seed，所有节点产生完全相同的结果。
#[derive(Debug, Clone)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);

        let mut value = self.state;

        value = (value ^ (value >> 30))
            .wrapping_mul(0xBF58_476D_1CE4_E5B9);

        value = (value ^ (value >> 27))
            .wrapping_mul(0x94D0_49BB_1331_11EB);

        value ^ (value >> 31)
    }

    /// 返回 [0, upper_bound) 范围内的下标。
    fn next_index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);

        /*
         * 使用乘法高位映射代替直接取模。
         *
         * 对于 SPOP 来说不要求密码学随机性。
         */
        let random = self.next_u64() as u128;
        let bound = upper_bound as u128;

        ((random * bound) >> 64) as usize
    }
}

impl SPopReq {
    /// 根据 Raft 日志中的确定性 write_clock 和 key 构造随机种子。
    ///
    /// 对 key 进行混合的作用是：即使两个不同 key 在相同逻辑时刻执行，
    /// 也尽量不会得到完全相同的随机序列。
    fn random_seed(&self, write_clock: u64) -> u64 {
        let mut hash = 0xCBF2_9CE4_8422_2325u64;

        // FNV-1a，用于稳定地混合 key 字节。
        for byte in self.key.as_ref() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }

        hash ^ write_clock.rotate_left(17) ^ 0xA076_1D64_78BD_642F
    }

    fn missing_key_response(&self) -> Value {
        match self.count {
            // SPOP key
            None => Value::BulkString(None),

            // SPOP key count: empty set reply, like Redis shared.emptyset.
            Some(_) => Value::Set(Vec::new()),
        }
    }

    fn popped_response(&self, members: Vec<Bytes>) -> Value {
        match self.count {
            None => {
                /*
                 * 没有 count 时，正常情况下只会弹出一个成员。
                 *
                 * 防御性处理空数组，避免潜在 panic。
                 */
                Value::BulkString(members.into_iter().next())
            }
            // Set reply for the count form (RESP2 *N, RESP3 ~N),
            // matching Redis addReplySetLen in spopWithCountCommand.
            Some(_) => Value::Set(
                members
                    .into_iter()
                    .map(|member| Value::BulkString(Some(member)))
                    .collect(),
            ),
        }
    }
}

impl ComputeCommand for SPopReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SPop(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Set(set) => {
                /*
                 * 即使 count 为 0，也必须先检查类型。
                 *
                 * Redis 对错误类型的 key 执行 SPOP key 0，
                 * 仍然应该返回 WRONGTYPE，而不是空数组。
                 */
                if self.count == Some(0) {
                    return (
                        MochaOperation::Abort,
                        Value::Set(Vec::new()),
                    );
                }

                let mut set_guard = set.lock();

                /*
                 * 正常情况下 Redis 不会保留空集合，因为最后一个元素被删除时，
                 * 整个 key 会被删除。
                 *
                 * 这里仍然处理空集合，防止历史数据或其他命令产生空 Set。
                 */
                if set_guard.is_empty() {
                    return (
                        MochaOperation::Remove,
                        self.missing_key_response(),
                    );
                }

                let set_len = set_guard.len();

                /*
                 * 未传 count 时弹出一个。
                 *
                 * 传 count 时最多弹出 min(count, cardinality) 个。
                 *
                 * 先与 set_len 比较，避免把一个非常大的 u64 count
                 * 直接转换成 usize。
                 */
                let pop_count = match self.count {
                    None => 1,

                    Some(count) => {
                        if count >= set_len as u64 {
                            set_len
                        } else {
                            count as usize
                        }
                    }
                };

                /*
                 * HashSet 的迭代顺序不稳定：
                 *
                 * - 不同进程的 RandomState 不同；
                 * - 不同 Raft 节点的迭代顺序可能不同；
                 * - 直接按 HashSet 迭代顺序随机选择会导致节点分叉。
                 *
                 * 因此必须先按字节序排序，建立所有节点一致的成员序列。
                 */
                let mut candidates: Vec<Bytes> =
                    set_guard.iter().cloned().collect();

                candidates.sort_unstable_by(|left, right| {
                    left.as_ref().cmp(right.as_ref())
                });

                let mut rng =
                    DeterministicRng::new(self.random_seed(write_clock));

                let mut popped = Vec::with_capacity(pop_count);

                /*
                 * 部分 Fisher-Yates 洗牌。
                 *
                 * 每轮从尚未选择的范围中随机挑一个成员，
                 * swap_remove 后不会重复选择，因此符合 SPOP：
                 * 每个成员最多返回一次。
                 */
                for _ in 0..pop_count {
                    let index = rng.next_index(candidates.len());
                    let member = candidates.swap_remove(index);

                    let removed = set_guard.remove(&member);

                    /*
                     * member 来自 set_guard 的快照，正常情况下一定能删除。
                     */
                    debug_assert!(removed);

                    popped.push(member);
                }

                let set_is_empty = set_guard.is_empty();

                drop(set_guard);

                let response = self.popped_response(popped);

                if set_is_empty {
                    /*
                     * Redis 在集合最后一个成员被弹出后删除 key。
                     */
                    (MochaOperation::Remove, response)
                } else {
                    /*
                     * 延续原 key 的过期策略。
                     */
                    (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        response,
                    )
                }
            }

            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        /*
         * key 不存在：
         *
         * SPOP key       -> nil bulk string
         * SPOP key count -> empty array
         */
        let response = self.missing_key_response();

        (MochaOperation::Abort, response)
    }
}
