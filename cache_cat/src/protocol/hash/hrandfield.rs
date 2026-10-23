//! HRANDFIELD command implementation.
//!
//! HRANDFIELD key [count [WITHVALUES]]
//! Returns one or more randomly selected fields from the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::set::random::DeterministicRng;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use rand::Rng;
use rand::seq::IteratorRandom;
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

        let entries: Vec<(&Bytes, &HashValue)> = guard.iter().collect();
        let mut rng = rand::thread_rng();
        self.sample(entries, &mut rng)
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

        // HashMap iteration order and hash seeds can differ between replicas.
        // Sorting the borrowed entries makes the logical-clock seeded sample
        // independent of those local details while avoiding value cloning.
        let mut entries: Vec<(&Bytes, &HashValue)> = guard.iter().collect();
        entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut rng = DeterministicRng::for_key(&self.key, read_clock);
        self.sample_deterministic(entries, &mut rng)
    }
}

impl HRandFieldParams {
    fn empty_result(&self) -> Value {
        match self.count {
            None => Value::BulkString(None),
            Some(_) => Value::Array(Some(Vec::new())),
        }
    }

    fn sample<R: Rng + ?Sized>(&self, entries: Vec<(&Bytes, &HashValue)>, rng: &mut R) -> Value {
        match self.count {
            None => entries
                .iter()
                .choose(rng)
                .map(|(field, _)| Value::BulkString(Some((*field).clone())))
                .unwrap_or(Value::BulkString(None)),
            Some(0) => Value::Array(Some(Vec::new())),
            Some(count) if count > 0 => {
                let take_count = (count as u64).min(entries.len() as u64) as usize;
                let selected = entries.iter().choose_multiple(rng, take_count);
                self.array_from_entries(selected)
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
                    let index = rng.gen_range(0..entries.len());
                    self.push_entry(&mut result, entries[index]);
                }
                Value::Array(Some(result))
            }
        }
    }

    fn sample_deterministic(
        &self,
        mut entries: Vec<(&Bytes, &HashValue)>,
        rng: &mut DeterministicRng,
    ) -> Value {
        match self.count {
            None => {
                let index = rng.next_index(entries.len());
                Value::BulkString(Some(entries[index].0.clone()))
            }
            Some(0) => Value::Array(Some(Vec::new())),
            Some(count) if count > 0 => {
                let take_count = (count as u64).min(entries.len() as u64) as usize;
                let mut result = Vec::new();
                let response_len = match self.response_len(take_count) {
                    Some(length) => length,
                    None => return Self::count_too_large(),
                };
                if result.try_reserve(response_len).is_err() {
                    return Self::count_too_large();
                }
                for _ in 0..take_count {
                    let index = rng.next_index(entries.len());
                    let entry = entries.swap_remove(index);
                    self.push_entry(&mut result, entry);
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
                    let index = rng.next_index(entries.len());
                    self.push_entry(&mut result, entries[index]);
                }
                Value::Array(Some(result))
            }
        }
    }

    fn array_from_entries<'a, I>(&self, entries: I) -> Value
    where
        I: IntoIterator<Item = &'a (&'a Bytes, &'a HashValue)>,
    {
        let selected: Vec<_> = entries.into_iter().copied().collect();
        let mut result = Vec::new();
        let response_len = match self.response_len(selected.len()) {
            Some(length) => length,
            None => return Self::count_too_large(),
        };
        if result.try_reserve(response_len).is_err() {
            return Self::count_too_large();
        }
        for entry in selected {
            self.push_entry(&mut result, entry);
        }
        Value::Array(Some(result))
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
    use parking_lot::Mutex;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    fn snapshot(reverse: bool, capacity: usize) -> EntrySnapshot<MyValue> {
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
        let mut hash = HashMap::with_capacity(capacity);
        let indexes = if reverse {
            vec![2, 1, 0]
        } else {
            vec![0, 1, 2]
        };
        for index in indexes {
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

    #[test]
    fn deterministic_sampling_ignores_hash_layout_and_preserves_values() {
        let left = snapshot(false, 0);
        let right = snapshot(true, 128);
        for (count, with_values) in [
            (None, false),
            (Some(0), false),
            (Some(2), false),
            (Some(100), true),
            (Some(-20), false),
            (Some(-20), true),
        ] {
            let command = params(count, with_values);
            assert_eq!(
                command
                    .execute_with_clock(Some(left.clone()), 12_345)
                    .encode(),
                command
                    .execute_with_clock(Some(right.clone()), 12_345)
                    .encode(),
                "different response for count {count:?}, with_values {with_values}"
            );
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
        assert_eq!(left.expire_at, Some(100_000));
        assert_eq!(left.value.version, 0);
    }

    #[test]
    fn count_sign_and_missing_semantics_match_redis() {
        let entry = snapshot(false, 0);
        let fields = [
            Bytes::from_static(b""),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"binary\0"),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        let single = params(None, false).execute_with_clock(Some(entry.clone()), 42);
        let Value::BulkString(Some(field)) = single else {
            panic!("HRANDFIELD without count must return one field")
        };
        assert!(fields.contains(&field));

        assert!(
            array(params(Some(0), false).execute_with_clock(Some(entry.clone()), 42)).is_empty()
        );
        let unique = array(params(Some(2), false).execute_with_clock(Some(entry.clone()), 42));
        assert_eq!(unique.len(), 2);
        assert_eq!(
            unique
                .iter()
                .map(Value::encode)
                .collect::<HashSet<_>>()
                .len(),
            2
        );

        let repeated = array(params(Some(-20), false).execute_with_clock(Some(entry.clone()), 42));
        assert_eq!(repeated.len(), 20);
        assert!(repeated.iter().all(|field| match field {
            Value::BulkString(Some(field)) => fields.contains(field),
            _ => false,
        }));

        assert_eq!(
            params(None, false).execute_with_clock(None, 42).encode(),
            b"$-1\r\n"
        );
        assert_eq!(
            params(Some(2), true).execute_with_clock(None, 42).encode(),
            b"*0\r\n"
        );
    }

    #[test]
    fn wrong_type_and_argument_validation_are_preserved() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        assert_eq!(
            params(Some(0), false)
                .execute_with_clock(Some(wrong_type), 42)
                .encode(),
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
