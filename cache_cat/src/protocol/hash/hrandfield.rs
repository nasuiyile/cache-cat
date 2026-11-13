//! HRANDFIELD command implementation.
//!
//! HRANDFIELD key [count [WITHVALUES]]
//! Returns one or more randomly selected fields from the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::set::random::{DeterministicRng, sample_unique_indices};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_map::ScanMap;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HRANDFIELD arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HRandFieldParams {
    pub key: Bytes,
    pub count: Option<i64>,
    pub with_values: bool,
}

impl Display for HRandFieldParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HRANDFIELD {}", String::from_utf8_lossy(&self.key))?;
        if let Some(count) = self.count {
            write!(f, " {count}")?;
            if self.with_values {
                write!(f, " WITHVALUES")?;
            }
        }
        Ok(())
    }
}

impl ReadCommand for HRandFieldParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(snapshot) = value else {
            return self.empty_result();
        };
        let ValueObject::Hash(hash) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        let guard = hash.lock();
        if guard.is_empty() {
            return self.empty_result();
        }

        let mut rng = rand::thread_rng();
        self.sample(&guard, |bound| rng.gen_range(0..bound))
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, read_clock: u64) -> Value {
        let Some(snapshot) = value else {
            return self.empty_result();
        };
        let ValueObject::Hash(hash) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        let guard = hash.lock();
        if guard.is_empty() {
            return self.empty_result();
        }

        // Replicas preserve dense field order through writes and snapshots.
        // Local hash seeds and bucket capacities cannot change index sampling.
        let mut rng = DeterministicRng::for_key(&self.key, read_clock);
        self.sample(&guard, |bound| rng.next_index(bound))
    }
}

impl HRandFieldParams {
    fn empty_result(&self) -> Value {
        match self.count {
            None => Value::BulkString(None),
            Some(_) => Value::Array(Some(Vec::new())),
        }
    }

    fn sample<F>(&self, hash: &ScanMap<Bytes, HashValue>, mut next_index: F) -> Value
    where
        F: FnMut(usize) -> usize,
    {
        match self.count {
            None => {
                let (field, _) = hash
                    .get_index(next_index(hash.len()))
                    .expect("sampled field index is valid");
                Value::BulkString(Some(field.clone()))
            }
            Some(0) => Value::Array(Some(Vec::new())),
            Some(count) if count > 0 => {
                let take_count = (count as u64).min(hash.len() as u64) as usize;
                let mut result = Vec::new();
                let response_len = match self.response_len(take_count) {
                    Some(length) => length,
                    None => return Self::count_too_large(),
                };
                if result.try_reserve(response_len).is_err() {
                    return Self::count_too_large();
                }
                if sample_unique_indices(hash.len(), take_count, &mut next_index, |index| {
                    self.push_entry(
                        &mut result,
                        hash.get_index(index).expect("sampled field index is valid"),
                    );
                })
                .is_err()
                {
                    return Self::count_too_large();
                }
                Value::Array(Some(result))
            }
            Some(count) => {
                let requested = match count
                    .checked_abs()
                    .and_then(|count| usize::try_from(count).ok())
                {
                    Some(count) => count,
                    None => return Self::count_out_of_range(),
                };
                let mut result = Vec::new();
                let response_len = match self.response_len(requested) {
                    Some(length) => length,
                    None => return Self::count_too_large(),
                };
                if result.try_reserve(response_len).is_err() {
                    return Self::count_too_large();
                }
                for _ in 0..requested {
                    let index = next_index(hash.len());
                    self.push_entry(
                        &mut result,
                        hash.get_index(index).expect("sampled field index is valid"),
                    );
                }
                Value::Array(Some(result))
            }
        }
    }

    fn push_entry(&self, result: &mut Vec<Value>, (field, value): (&Bytes, &HashValue)) {
        result.push(Value::BulkString(Some(field.clone())));
        if self.with_values {
            result.push(Value::BulkString(Some(value.to_bytes())));
        }
    }

    fn response_len(&self, count: usize) -> Option<usize> {
        if self.with_values {
            count.checked_mul(2)
        } else {
            Some(count)
        }
    }

    fn count_out_of_range() -> Value {
        ProtocolError::response(
            "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
        )
        .into()
    }

    fn count_too_large() -> Value {
        ProtocolError::response("ERR count is too large").into()
    }
}

/// HRANDFIELD command handler.
pub struct HRandFieldCommand;

impl HRandFieldCommand {
    fn parse_args(items: &[Value]) -> Result<HRandFieldParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hrandfield"));
        }
        if items.len() > 4 {
            return Err(ProtocolError::SyntaxError);
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        if items.len() == 2 {
            return Ok(HRandFieldParams {
                key,
                count: None,
                with_values: false,
            });
        }

        let count = items[2].try_parse_canonical_i64()?;
        if count == i64::MIN {
            return Err(ProtocolError::response(
                "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
            ));
        }
        let with_values = if items.len() == 4 {
            let option = items[3]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if !option_eq(&option, b"WITHVALUES") {
                return Err(ProtocolError::SyntaxError);
            }
            true
        } else {
            false
        };

        Ok(HRandFieldParams {
            key,
            count: Some(count),
            with_values,
        })
    }
}

impl ReadRaftCommand for HRandFieldCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HRandField(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HRandFieldCommand {
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
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::scan_map::ScanMap as HashMap;
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn snapshot(seed: u64, capacity: usize) -> EntrySnapshot<MyValue> {
        let fields = [
            (
                Bytes::from_static(b""),
                HashValue::Str(Bytes::from_static(b"zero")),
            ),
            (Bytes::from_static(b"a"), HashValue::Int(-4)),
            (
                Bytes::from_static(b"binary\0"),
                HashValue::Str(Bytes::from_static(b"v\0\xff")),
            ),
        ];
        let mut hash = HashMap::with_capacity_and_hasher(
            capacity,
            ahash::RandomState::with_seeds(seed, 2, 3, 4),
        );
        // Keep the replicated dense sequence identical and deliberately unsorted.
        for index in [2, 0, 1] {
            let (field, value) = &fields[index];
            hash.insert(field.clone(), value.clone());
        }
        EntrySnapshot {
            value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(hash)))),
            expire_at: Some(100_000),
        }
    }

    fn params(count: Option<i64>, with_values: bool) -> HRandFieldParams {
        HRandFieldParams {
            key: Bytes::from_static(b"hash"),
            count,
            with_values,
        }
    }

    fn array(result: Value) -> Vec<Value> {
        let Value::Array(Some(values)) = result else {
            panic!("expected array reply, got {result:?}");
        };
        values
    }

    fn encoded_hash(entry: &EntrySnapshot<MyValue>) -> Vec<u8> {
        let ValueObject::Hash(hash) = &entry.value.data else {
            panic!("expected hash");
        };
        bincode2::serialize(&*hash.lock()).unwrap()
    }

    #[test]
    fn deterministic_sampling_preserves_dense_order_across_hash_layouts_and_restore() {
        let left = snapshot(10, 0);
        let right = snapshot(20, 128);
        let before = encoded_hash(&left);
        let restored: HashMap<Bytes, HashValue> = bincode2::deserialize(&before).unwrap();
        let restored = EntrySnapshot {
            value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(restored)))),
            expire_at: left.expire_at,
        };
        for (count, with_values) in [
            (None, false),
            (Some(0), false),
            (Some(1), false),
            (Some(1), true),
            (Some(2), false),
            (Some(2), true),
            (Some(100), true),
            (Some(-20), false),
            (Some(-20), true),
        ] {
            let command = params(count, with_values);
            for replica in [&right, &restored] {
                assert_eq!(
                    command
                        .execute_with_clock(Some(left.clone()), 12_345)
                        .encode(),
                    command
                        .execute_with_clock(Some((*replica).clone()), 12_345)
                        .encode(),
                    "different response for count {count:?}, with_values {with_values}"
                );
            }
        }

        let result = array(params(Some(2), true).execute_with_clock(Some(left.clone()), 42));
        assert_eq!(result.len(), 4);
        let fields = HashMap::from([
            (Bytes::from_static(b""), Bytes::from_static(b"zero")),
            (Bytes::from_static(b"a"), Bytes::from_static(b"-4")),
            (
                Bytes::from_static(b"binary\0"),
                Bytes::from_static(b"v\0\xff"),
            ),
        ]);
        for pair in result.chunks_exact(2) {
            let Value::BulkString(Some(field)) = &pair[0] else {
                panic!("expected field bulk string")
            };
            let Value::BulkString(Some(value)) = &pair[1] else {
                panic!("expected value bulk string")
            };
            assert_eq!(fields.get(field), Some(value));
        }
        assert_eq!(encoded_hash(&left), before);
        assert_eq!(encoded_hash(&right), before);
        assert_eq!(encoded_hash(&restored), before);
        assert_eq!(left.expire_at, Some(100_000));
        assert_eq!(left.value.version, 0);
    }

    #[test]
    fn indexed_sampling_uses_dense_fields_and_never_modifies_the_hash() {
        let entry = snapshot(10, 128);
        let before = encoded_hash(&entry);
        let ValueObject::Hash(hash) = &entry.value.data else {
            panic!("expected hash");
        };
        {
            let guard = hash.lock();
            assert_eq!(
                params(None, false).sample(&guard, |_| 0).encode(),
                b"$7\r\nbinary\0\r\n"
            );
            let mut index = 0;
            let repeated = params(Some(-3), true).sample(&guard, |bound| {
                let selected = index % bound;
                index += 1;
                selected
            });
            let expected = b"*6\r\n$7\r\nbinary\0\r\n$3\r\nv\0\xff\r\n$0\r\n\r\n$4\r\nzero\r\n$1\r\na\r\n$2\r\n-4\r\n";
            assert_eq!(repeated.encode(), expected);
            let all = params(Some(i64::MAX), true).sample(&guard, |_| {
                panic!("returning all fields needs no random draw")
            });
            assert_eq!(all.encode(), expected);
        }
        for command in [
            params(None, false),
            params(Some(2), true),
            params(Some(-20), true),
        ] {
            command.execute(Some(entry.clone()));
            command.execute_with_clock(Some(entry.clone()), 42);
        }
        assert_eq!(encoded_hash(&entry), before);
    }

    #[test]
    fn count_sign_and_missing_semantics_match_redis() {
        let entry = snapshot(10, 0);
        let fields = [
            Bytes::from_static(b""),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"binary\0"),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        for deterministic in [false, true] {
            let execute = |command: HRandFieldParams, value| {
                if deterministic {
                    command.execute_with_clock(value, 42)
                } else {
                    command.execute(value)
                }
            };
            let single = execute(params(None, false), Some(entry.clone()));
            let Value::BulkString(Some(field)) = single else {
                panic!("HRANDFIELD without count must return one field")
            };
            assert!(fields.contains(&field));

            assert!(array(execute(params(Some(0), false), Some(entry.clone()))).is_empty());
            let unique = array(execute(params(Some(2), false), Some(entry.clone())));
            assert_eq!(unique.len(), 2);
            assert_eq!(
                unique
                    .iter()
                    .map(Value::encode)
                    .collect::<HashSet<_>>()
                    .len(),
                2
            );

            let repeated = array(execute(params(Some(-20), false), Some(entry.clone())));
            assert_eq!(repeated.len(), 20);
            assert!(repeated.iter().all(|field| match field {
                Value::BulkString(Some(field)) => fields.contains(field),
                _ => false,
            }));
            assert_eq!(execute(params(None, false), None).encode(), b"$-1\r\n");
            assert_eq!(execute(params(Some(2), true), None).encode(), b"*0\r\n");

            let empty = EntrySnapshot {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(HashMap::new())))),
                expire_at: None,
            };
            assert_eq!(
                execute(params(None, false), Some(empty.clone())).encode(),
                b"$-1\r\n"
            );
            assert!(array(execute(params(Some(-20), true), Some(empty))).is_empty());
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn huge_negative_counts_return_the_existing_allocation_error() {
        let entry = snapshot(10, 0);
        for with_values in [false, true] {
            let command = params(Some(-i64::MAX), with_values);
            assert_eq!(
                command.execute(Some(entry.clone())).encode(),
                b"-ERR count is too large\r\n"
            );
            assert_eq!(
                command.execute_with_clock(Some(entry.clone()), 42).encode(),
                b"-ERR count is too large\r\n"
            );
        }
    }

    #[test]
    fn wrong_type_and_argument_validation_are_preserved() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let command = params(Some(0), false);
        assert_eq!(
            command.execute(Some(wrong_type.clone())).encode(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
        assert_eq!(
            command.execute_with_clock(Some(wrong_type), 42).encode(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );

        let command = HRandFieldCommand;
        let base = || {
            vec![
                Value::SimpleString("HRANDFIELD".into()),
                Value::SimpleString("key".into()),
            ]
        };
        assert!(command.read_operation(&base()).is_ok());

        let mut with_values = base();
        with_values.extend([
            Value::SimpleString("2".into()),
            Value::SimpleString("wItHvAlUeS".into()),
        ]);
        assert!(
            command
                .read_operation(&with_values)
                .unwrap()
                .matches_with_values()
        );

        for option in ["WITHVALUES\0", "wItHvAlUeS\0suffix"] {
            let mut nul_terminated = base();
            nul_terminated.extend([
                Value::SimpleString("2".into()),
                Value::BulkString(Some(Bytes::copy_from_slice(option.as_bytes()))),
            ]);
            assert!(
                command
                    .read_operation(&nul_terminated)
                    .unwrap()
                    .matches_with_values(),
                "option {option:?}"
            );
        }

        let mut invalid_option = with_values.clone();
        invalid_option[3] = Value::SimpleString("WITHSCORES".into());
        assert_eq!(
            command
                .read_operation(&invalid_option)
                .unwrap_err()
                .to_string(),
            "ERR syntax error"
        );

        let mut min_count = base();
        min_count.push(Value::SimpleString(i64::MIN.to_string()));
        assert!(command.read_operation(&min_count).is_err());
        let mut extra = base();
        extra.extend([
            Value::SimpleString("1".into()),
            Value::SimpleString("WITHVALUES".into()),
            Value::SimpleString("extra".into()),
        ]);
        assert_eq!(
            command.read_operation(&extra).unwrap_err().to_string(),
            "ERR syntax error"
        );
    }

    trait ParsedHRandField {
        fn matches_with_values(&self) -> bool;
    }

    impl ParsedHRandField for ReadOperation {
        fn matches_with_values(&self) -> bool {
            matches!(self, ReadOperation::HRandField(params) if params.with_values)
        }
    }
}
