use super::random::DeterministicRng;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub struct SRandMemberCommand;

/// SRANDMEMBER 的 count 参数是否存在，会影响返回类型：
///
/// SRANDMEMBER key
///     -> BulkString
///
/// SRANDMEMBER key count
///     -> Array
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRandMemberParams {
    pub key: Bytes,
    pub count: Option<i64>,
}

impl Display for SRandMemberParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.count {
            Some(count) => write!(
                f,
                "SRandMemberParams {{ key: {}, count: {} }}",
                String::from_utf8_lossy(&self.key),
                count
            ),
            None => write!(
                f,
                "SRandMemberParams {{ key: {}, count: None }}",
                String::from_utf8_lossy(&self.key)
            ),
        }
    }
}

impl ReadCommand for SRandMemberParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => self.empty_result(),

            Some(snapshot) => match snapshot.value.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();

                    if guard.is_empty() {
                        return self.empty_result();
                    }

                    match self.count {
                        // SRANDMEMBER key
                        //
                        // 不带 count 时返回单个 BulkString；
                        // 集合为空时返回 Nil BulkString。
                        None => {
                            let mut rng = rand::thread_rng();

                            match guard.iter().choose(&mut rng) {
                                Some(member) => Value::BulkString(Some(member.clone())),
                                None => Value::BulkString(None),
                            }
                        }

                        // SRANDMEMBER key 0
                        Some(0) => Value::Array(Some(Vec::new())),

                        // SRANDMEMBER key positive-count
                        //
                        // 返回不重复的随机元素。
                        // 如果 count 大于集合大小，只返回集合中的全部元素。
                        Some(count) if count > 0 => {
                            let requested = match usize::try_from(count) {
                                Ok(count) => count,
                                Err(_) => {
                                    return Value::Array(Some(Vec::new()));
                                }
                            };

                            let take_count = requested.min(guard.len());
                            let mut rng = rand::thread_rng();

                            let members = guard
                                .iter()
                                .choose_multiple(&mut rng, take_count)
                                .into_iter()
                                .map(|member| Value::BulkString(Some(member.clone())))
                                .collect();

                            Value::Array(Some(members))
                        }

                        // SRANDMEMBER key negative-count
                        //
                        // 允许返回重复元素，并且必须返回 abs(count) 个元素。
                        Some(count) => {
                            let requested = match count
                                .checked_abs()
                                .and_then(|count| usize::try_from(count).ok())
                            {
                                Some(count) => count,

                                // i64::MIN 无法使用有符号 i64 表示其绝对值。
                                // 正常情况下也不应允许客户端要求如此巨大的响应。
                                None => {
                                    return ProtocolError::response(
                                        "ERR value is out of range, must be positive",
                                    )
                                    .into();
                                }
                            };

                            let members: Vec<&Bytes> = guard.iter().collect();
                            let mut rng = rand::thread_rng();

                            let mut result = Vec::new();

                            // 避免一次 reserve 巨大内存时直接 panic。
                            if result.try_reserve(requested).is_err() {
                                return ProtocolError::response("ERR count is too large").into();
                            }

                            for _ in 0..requested {
                                let index = rng.gen_range(0..members.len());

                                result.push(Value::BulkString(Some(members[index].clone())));
                            }

                            Value::Array(Some(result))
                        }
                    }
                }

                _ => ProtocolError::WrongType.into(),
            },
        }
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, read_clock: u64) -> Value {
        let Some(snapshot) = value else {
            return self.empty_result();
        };
        let ValueObject::Set(set) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        if self.count == Some(0) {
            return Value::Array(Some(Vec::new()));
        }
        let guard = set.lock();
        if guard.is_empty() {
            return self.empty_result();
        }
        let requested = match self.count {
            None => 1,
            Some(count) if count > 0 => (count as u64).min(guard.len() as u64) as usize,
            Some(count) => match count
                .checked_abs()
                .and_then(|count| usize::try_from(count).ok())
            {
                Some(count) => count,
                None => {
                    return ProtocolError::response("ERR value is out of range, must be positive")
                        .into();
                }
            },
        };

        // Sorting borrowed members removes HashSet layout and seed differences
        // without cloning every member. Only returned members are cloned.
        let mut members: Vec<&Bytes> = guard.iter().collect();
        members.sort_unstable();
        let mut rng = DeterministicRng::for_key(&self.key, read_clock);
        if self.count.is_none() {
            return Value::BulkString(Some(members[rng.next_index(members.len())].clone()));
        }

        let mut result = Vec::new();
        if result.try_reserve(requested).is_err() {
            return ProtocolError::response("ERR count is too large").into();
        }
        let allow_duplicates = self.count.is_some_and(|count| count < 0);
        for _ in 0..requested {
            let index = rng.next_index(members.len());
            let member = if allow_duplicates {
                members[index]
            } else {
                // Partial Fisher-Yates: a positive count cannot repeat a member.
                members.swap_remove(index)
            };
            result.push(Value::BulkString(Some(member.clone())));
        }
        Value::Array(Some(result))
    }
}

impl SRandMemberParams {
    /// key 不存在或集合为空时：
    ///
    /// 不带 count：
    ///     返回 Nil BulkString。
    ///
    /// 带 count：
    ///     返回空数组。
    fn empty_result(&self) -> Value {
        match self.count {
            None => Value::BulkString(None),
            Some(_) => Value::Array(Some(Vec::new())),
        }
    }
}

impl SRandMemberCommand {
    fn parse_args(items: &[Value]) -> Result<SRandMemberParams, ProtocolError> {
        if items.len() != 2 && items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("srandmember"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let count = if items.len() == 3 {
            let count_bytes = items[2]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("count"))?;

            let count_str = std::str::from_utf8(count_bytes.as_ref())
                .map_err(|_| ProtocolError::InvalidArgument("count"))?;

            let count = count_str
                .parse::<i64>()
                .map_err(|_| ProtocolError::InvalidArgument("count"))?;

            Some(count)
        } else {
            None
        };

        Ok(SRandMemberParams { key, count })
    }
}

impl ReadRaftCommand for SRandMemberCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SRandMember(SRandMemberCommand::parse_args(
            items,
        )?))
    }
}

#[async_trait]
impl Command for SRandMemberCommand {
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

        let operation = self.read_operation(items)?;

        server.app.read(operation, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn snapshot(reverse: bool, capacity: usize) -> EntrySnapshot<MyValue> {
        let mut members: Vec<Bytes> = [
            b"".as_slice(),
            b"z".as_slice(),
            b"10".as_slice(),
            b"\xff".as_slice(),
            b"a\0".as_slice(),
            b"\x80".as_slice(),
        ]
        .into_iter()
        .map(Bytes::copy_from_slice)
        .collect();
        if reverse {
            members.reverse();
        }
        let mut set = HashSet::with_capacity(capacity);
        set.extend(members);
        EntrySnapshot {
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
            expire_at: Some(100_000),
        }
    }

    fn params(count: Option<i64>) -> SRandMemberParams {
        SRandMemberParams {
            key: Bytes::from_static(b"bag"),
            count,
        }
    }

    fn array_members(result: Value) -> Vec<Bytes> {
        let Value::Array(Some(values)) = result else {
            panic!("expected SRANDMEMBER count array, got {result:?}");
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::BulkString(Some(bytes)) => bytes,
                other => panic!("expected bulk string, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn deterministic_sampling_ignores_hash_layout_and_preserves_set_and_ttl() {
        let left = snapshot(false, 8);
        let right = snapshot(true, 128);
        let ValueObject::Set(set) = &left.value.data else {
            unreachable!();
        };
        let original = set.lock().clone();

        for count in [None, Some(0), Some(1), Some(4), Some(100), Some(-20)] {
            let command = params(count);
            assert_eq!(
                command
                    .execute_with_clock(Some(left.clone()), 12_345)
                    .encode(),
                command
                    .execute_with_clock(Some(right.clone()), 12_345)
                    .encode(),
                "different response for count {count:?}"
            );
        }
        assert_eq!(*set.lock(), original);
        assert_eq!(left.expire_at, Some(100_000));
        assert_eq!(left.value.version, 1);
    }

    #[test]
    fn deterministic_count_sign_preserves_sampling_semantics() {
        let entry = snapshot(false, 8);
        let unique = array_members(params(Some(4)).execute_with_clock(Some(entry.clone()), 42));
        assert_eq!(unique.len(), 4);
        assert_eq!(unique.iter().collect::<HashSet<_>>().len(), 4);

        let all = array_members(params(Some(i64::MAX)).execute_with_clock(Some(entry.clone()), 42));
        assert_eq!(all.len(), 6);
        assert_eq!(all.iter().collect::<HashSet<_>>().len(), 6);

        let repeated = array_members(params(Some(-20)).execute_with_clock(Some(entry.clone()), 42));
        assert_eq!(repeated.len(), 20);
        assert!(repeated.iter().collect::<HashSet<_>>().len() < repeated.len());
        assert!(repeated.iter().all(|member| all.contains(member)));

        let Value::BulkString(Some(member)) = params(None).execute_with_clock(Some(entry), 42)
        else {
            panic!("SRANDMEMBER without count must return a bulk string");
        };
        assert!(all.contains(&member));
    }

    #[test]
    fn deterministic_sampling_uses_the_logical_clock() {
        let entry = snapshot(false, 8);
        let command = params(Some(-20));
        let sequences: HashSet<_> = (1..=16)
            .map(|clock| {
                command
                    .execute_with_clock(Some(entry.clone()), clock)
                    .encode()
            })
            .collect();
        assert!(sequences.len() > 1);
    }

    #[test]
    fn deterministic_sampling_preserves_missing_wrong_type_and_overflow_replies() {
        let empty = EntrySnapshot {
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(HashSet::new())))),
            expire_at: None,
        };
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        for count in [None, Some(0), Some(2), Some(-2), Some(i64::MIN)] {
            let command = params(count);
            for entry in [None, Some(empty.clone()), Some(wrong_type.clone())] {
                assert_eq!(
                    command.execute_with_clock(entry.clone(), 42).encode(),
                    command.execute(entry).encode()
                );
            }
        }
        let command = params(Some(i64::MIN));
        let nonempty = snapshot(false, 8);
        assert_eq!(
            command
                .execute_with_clock(Some(nonempty.clone()), 42)
                .encode(),
            command.execute(Some(nonempty)).encode()
        );
    }
}
