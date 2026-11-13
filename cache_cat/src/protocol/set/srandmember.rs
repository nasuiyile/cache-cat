use super::random::{DeterministicRng, sample_unique_indices};
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
        let mut rng = rand::thread_rng();
        self.sample(value, |bound| rng.gen_range(0..bound))
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, read_clock: u64) -> Value {
        let mut rng = DeterministicRng::for_key(&self.key, read_clock);
        self.sample(value, |bound| rng.next_index(bound))
    }
}
impl SRandMemberParams {
    fn sample<F>(&self, value: Option<EntrySnapshot<MyValue>>, mut next_index: F) -> Value
    where
        F: FnMut(usize) -> usize,
    {
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
        // Read-only sampling must not swap the replicated member array itself.
        let Some(count) = self.count else {
            let member = guard
                .get_index(next_index(guard.len()))
                .expect("sampled index is within the locked set");
            return Value::BulkString(Some(member.clone()));
        };
        let requested = if count > 0 {
            (count as u64).min(guard.len() as u64) as usize
        } else {
            match count
                .checked_abs()
                .and_then(|count| usize::try_from(count).ok())
            {
                Some(count) => count,
                None => {
                    return ProtocolError::response("ERR value is out of range, must be positive")
                        .into();
                }
            }
        };
        let mut result = Vec::new();
        if result.try_reserve(requested).is_err() {
            return ProtocolError::response("ERR count is too large").into();
        }
        let mut append_member = |index| {
            let member = guard
                .get_index(index)
                .expect("sampled index is within the locked set");
            result.push(Value::BulkString(Some(member.clone())));
        };
        if count > 0 {
            if sample_unique_indices(guard.len(), requested, next_index, append_member).is_err() {
                return ProtocolError::response("ERR count is too large").into();
            }
        } else {
            for _ in 0..requested {
                append_member(next_index(guard.len()));
            }
        }
        Value::Array(Some(result))
    }

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
        if items.len() > 3 {
            // Redis's command handler reports syntax errors for extra
            // positional arguments rather than the generic wrong-argc reply.
            return Err(ProtocolError::SyntaxError);
        }
        if items.len() != 2 && items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("srandmember"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let count = if items.len() == 3 {
            // Redis getRangeLongFromObjectOrReply accepts only canonical
            // signed integers and excludes INT64_MIN (the lower bound is
            // -LONG_MAX because its absolute value is used for sampling).
            let count = items[2].try_parse_canonical_i64()?;
            if count == i64::MIN {
                return Err(ProtocolError::response(
                    "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                ));
            }
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
    use crate::raft::types::core::structure::scan_set::ScanSet;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn snapshot(restore: bool, capacity: usize) -> EntrySnapshot<MyValue> {
        let members: Vec<Bytes> = [
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
        let seed = capacity as u64;
        let mut set = ScanSet::with_capacity_and_hasher(
            capacity,
            ahash::RandomState::with_seeds(seed, seed + 1, seed + 2, seed + 3),
        );
        set.extend(members);
        if restore {
            set = bincode2::deserialize(&bincode2::serialize(&set).unwrap()).unwrap();
        }
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
        let original = set.lock().iter().cloned().collect::<Vec<_>>();

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
        assert_eq!(set.lock().iter().cloned().collect::<Vec<_>>(), original);
        assert_eq!(left.expire_at, Some(100_000));
        assert_eq!(left.value.version, 0);
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
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(ScanSet::new())))),
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

    #[test]
    fn count_uses_redis_canonical_range() {
        let mut items = vec![
            Value::SimpleString("SRANDMEMBER".into()),
            Value::SimpleString("key".into()),
            Value::SimpleString("1".into()),
        ];
        assert!(SRandMemberCommand::parse_args(&items).is_ok());
        for count in [
            "",
            "01",
            "+1",
            "-0",
            "-01",
            " 1",
            "1 ",
            "1\0",
            "9223372036854775808",
        ] {
            items[2] = Value::BulkString(Some(Bytes::copy_from_slice(count.as_bytes())));
            assert_eq!(
                SRandMemberCommand::parse_args(&items)
                    .unwrap_err()
                    .to_string(),
                "ERR value is not an integer or out of range"
            );
        }
        for count in [
            "0",
            "1",
            "-1",
            "9223372036854775807",
            "-9223372036854775807",
        ] {
            items[2] = Value::BulkString(Some(Bytes::copy_from_slice(count.as_bytes())));
            assert_eq!(
                SRandMemberCommand::parse_args(&items).unwrap().count,
                Some(count.parse::<i64>().unwrap())
            );
        }
        items[2] = Value::BulkString(Some(Bytes::from_static(b"-9223372036854775808")));
        assert_eq!(
            SRandMemberCommand::parse_args(&items)
                .unwrap_err()
                .to_string(),
            "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807"
        );

        items.push(Value::SimpleString("extra".into()));
        assert_eq!(
            SRandMemberCommand::parse_args(&items)
                .unwrap_err()
                .to_string(),
            "ERR syntax error"
        );
    }
}
