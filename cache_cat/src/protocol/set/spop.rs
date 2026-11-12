use super::random::DeterministicRng;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::base_operation::BaseOperation::SPop;
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
        let count = value.try_parse_canonical_i64()?;

        if count < 0 {
            return Err(ProtocolError::response(
                "ERR value is out of range, must be positive",
            ));
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

        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SPopReq {
    pub key: Bytes,

    /// None 表示没有传 count，响应必须是 BulkString。
    ///
    /// Some(count) 返回集合响应（RESP2 Array，RESP3 Set），
    /// 即使 count 为 1，也仍然返回集合响应。
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

impl SPopReq {
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
                    return (MochaOperation::Abort, Value::Set(Vec::new()));
                }

                let mut set_guard = set.lock();

                /*
                 * 正常情况下 Redis 不会保留空集合，因为最后一个元素被删除时，
                 * 整个 key 会被删除。
                 *
                 * 这里仍然处理空集合，防止历史数据或其他命令产生空 Set。
                 */
                if set_guard.is_empty() {
                    return (MochaOperation::Remove, self.missing_key_response());
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

                let mut rng = DeterministicRng::for_key(&self.key, write_clock);
                let mut popped = Vec::with_capacity(pop_count);

                // ScanSet preserves dense member order across replicated writes,
                // snapshot clones and restoration. Hash buckets may differ by
                // node, but the same clock selects the same dense index. Sampling
                // and swap-removing in place takes expected O(pop_count), without
                // collecting or sorting the entire set, and never repeats a member.
                for _ in 0..pop_count {
                    let index = rng.next_index(set_guard.len());
                    let member = set_guard
                        .swap_remove_index(index)
                        .expect("sampled index is within the locked set");
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

            _ => (MochaOperation::Abort, ProtocolError::WrongType.into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::scan_set::ScanSet;
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[test]
    fn count_uses_redis_integer_syntax_and_rejects_negative_values() {
        for count in ["+1", "00", "01", "-0", "9223372036854775808"] {
            let items = ["SPOP", "set", count]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert_eq!(
                SPopCommand.raft_request(&items).unwrap_err(),
                ProtocolError::NotAnInteger
            );
        }
        for count in ["0", "1", "9223372036854775807"] {
            let items = ["SPOP", "set", count]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert!(SPopCommand.raft_request(&items).is_ok());
        }
        let count = Value::BulkString(Some(Bytes::from_static(b"-1")));
        assert_eq!(
            SPopCommand::parse_count(&count).unwrap_err().to_string(),
            "ERR value is out of range, must be positive"
        );
    }

    #[test]
    fn samples_dense_member_order_and_preserves_ttl() {
        let set: ScanSet<Bytes> = ["h", "g", "f", "e", "d", "c", "b", "a"]
            .into_iter()
            .map(|value| Bytes::from_static(value.as_bytes()))
            .collect();
        let set = Arc::new(Mutex::new(set));
        let entry = EntrySnapshot {
            value: MyValue::new(ValueObject::Set(set.clone())),
            expire_at: Some(100_000),
        };
        let (operation, result) = SPopReq {
            key: Bytes::from_static(b"bag"),
            count: Some(3),
        }
        .mutate(entry, 12_345);

        assert_eq!(result.encode(), b"*3\r\n$1\r\ne\r\n$1\r\na\r\n$1\r\ng\r\n");
        assert_eq!(
            result.encode_proto(3),
            b"~3\r\n$1\r\ne\r\n$1\r\na\r\n$1\r\ng\r\n"
        );
        assert!(matches!(
            operation,
            MochaOperation::Insert {
                expire: crate::mocha::ExpirePolicy::Absolute(100_000),
                ..
            }
        ));
        let expected: Vec<Bytes> = ["h", "c", "f", "b", "d"]
            .into_iter()
            .map(|value| Bytes::from_static(value.as_bytes()))
            .collect();
        assert_eq!(set.lock().iter().cloned().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn count_forms_preserve_reply_types_and_delete_empty_sets() {
        for count in [None, Some(0), Some(1), Some(i64::MAX as u64)] {
            let set = Arc::new(Mutex::new(ScanSet::from([Bytes::from_static(b"member")])));
            let entry = EntrySnapshot {
                value: MyValue::new(ValueObject::Set(set.clone())),
                expire_at: None,
            };
            let request = SPopReq {
                key: Bytes::from_static(b"bag"),
                count,
            };
            let (operation, response) = request.clone().mutate(entry, 123);
            if count == Some(0) {
                assert!(matches!(operation, MochaOperation::Abort));
                assert_eq!(set.lock().len(), 1);
                assert_eq!(response.encode_proto(2), b"*0\r\n");
                assert_eq!(response.encode_proto(3), b"~0\r\n");
            } else {
                assert!(matches!(operation, MochaOperation::Remove));
                assert!(set.lock().is_empty());
                if count.is_none() {
                    assert_eq!(response.encode_proto(2), b"$6\r\nmember\r\n");
                    assert_eq!(response.encode_proto(3), b"$6\r\nmember\r\n");
                } else {
                    assert_eq!(response.encode_proto(2), b"*1\r\n$6\r\nmember\r\n");
                    assert_eq!(response.encode_proto(3), b"~1\r\n$6\r\nmember\r\n");
                }
            }

            let (operation, missing) = request.init();
            assert!(matches!(operation, MochaOperation::Abort));
            if count.is_none() {
                assert_eq!(missing.encode_proto(2), b"$-1\r\n");
                assert_eq!(missing.encode_proto(3), b"_\r\n");
            } else {
                assert_eq!(missing.encode_proto(2), b"*0\r\n");
                assert_eq!(missing.encode_proto(3), b"~0\r\n");
            }
        }
    }

    #[test]
    fn zero_count_still_checks_the_value_type() {
        let (operation, response) = SPopReq {
            key: Bytes::from_static(b"bag"),
            count: Some(0),
        }
        .mutate(
            EntrySnapshot {
                value: MyValue::new(ValueObject::Int(1)),
                expire_at: None,
            },
            123,
        );
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(
            response.encode(),
            Value::from(ProtocolError::WrongType).encode()
        );
    }
}
