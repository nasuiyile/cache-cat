//! HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES].

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use crate::utils::parse_canonical_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    pub no_values: bool,
    // Redis checks the cursor before lookup, but validates options only after
    // finding a hash. Preserve that order, including inside EXEC and Lua.
    option_error: Option<String>,
}

impl Display for HScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HSCAN {} {} COUNT {}",
            String::from_utf8_lossy(&self.key),
            self.cursor,
            self.count
        )?;
        if let Some(pattern) = &self.pattern {
            write!(f, " MATCH {}", String::from_utf8_lossy(pattern))?;
        }
        if self.no_values {
            write!(f, " NOVALUES")?;
        }
        Ok(())
    }
}

impl ReadCommand for HScanParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        self.execute_scan::<false>(value)
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, _read_clock: u64) -> Value {
        self.execute_scan::<true>(value)
    }
}

impl HScanParams {
    fn execute_scan<const DETERMINISTIC: bool>(
        &self,
        value: Option<EntrySnapshot<MyValue>>,
    ) -> Value {
        let Some(snapshot) = value else {
            return Self::reply(0, Vec::new());
        };
        let ValueObject::Hash(hash) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        if let Some(error) = &self.option_error {
            return Value::error(error.clone());
        }
        let guard = hash.lock();
        let matcher = self.pattern.as_deref().map(GlobMatcher::new);
        let mut result = Vec::with_capacity(
            self.count.min(guard.len()).min(256) * if self.no_values { 1 } else { 2 },
        );

        if DETERMINISTIC {
            // Replicated scripts may use this result to decide subsequent writes.
            // Local hash seeds, capacities and bucket orders are not replicated.
            // Return a complete, sorted scan in this path (COUNT is a hint),
            // accepting even a cursor produced by a previous ordinary scan.
            // This costs O(N log N) only in EXEC/Lua, and needs no cursor state.
            let mut entries: Vec<_> = guard
                .iter()
                .filter(|(field, _)| {
                    matcher
                        .as_ref()
                        .is_none_or(|pattern| pattern.matches(field))
                })
                .collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (field, value) in entries {
                self.push_entry(&mut result, field, value);
            }
            return Self::reply(0, result);
        }

        let cursor = guard.scan(self.cursor, self.count, |field, value| {
            if matcher
                .as_ref()
                .is_none_or(|pattern| pattern.matches(field))
            {
                self.push_entry(&mut result, field, value);
            }
        });
        Self::reply(cursor, result)
    }

    fn push_entry(&self, result: &mut Vec<Value>, field: &Bytes, value: &HashValue) {
        result.push(Value::BulkString(Some(field.clone())));
        if !self.no_values {
            result.push(Value::BulkString(Some(value.to_bytes())));
        }
    }

    fn reply(cursor: u64, entries: Vec<Value>) -> Value {
        Value::Array(Some(vec![
            Value::BulkString(Some(cursor.to_string().into())),
            Value::Array(Some(entries)),
        ]))
    }
}

pub struct HScanCommand;

impl HScanCommand {
    fn parse_args(items: &[Value]) -> Result<HScanParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hscan"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let cursor = items[2]
            .string_bytes_clone()
            .and_then(|value| parse_cursor(&value))
            .ok_or(ProtocolError::InvalidArgument("cursor"))?;
        let mut params = HScanParams {
            key,
            cursor,
            count: 10,
            pattern: None,
            no_values: false,
            option_error: None,
        };
        if let Err(error) = Self::parse_options(&mut params, &items[3..]) {
            params.option_error = Some(error.to_string());
        }
        Ok(params)
    }

    fn parse_options(params: &mut HScanParams, items: &[Value]) -> Result<(), ProtocolError> {
        let mut index = 0;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"COUNT") && index + 1 < items.len() {
                let count = items[index + 1].try_parse_canonical_i64()?;
                if count < 1 {
                    return Err(ProtocolError::SyntaxError);
                }
                params.count = usize::try_from(count).map_err(|_| ProtocolError::NotAnInteger)?;
                index += 2;
            } else if option_eq(&option, b"MATCH") && index + 1 < items.len() {
                let pattern = items[index + 1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::SyntaxError)?;
                params.pattern = (pattern.as_ref() != b"*").then_some(pattern);
                index += 2;
            } else if option_eq(&option, b"NOVALUES") {
                params.no_values = true;
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        Ok(())
    }
}

/// Redis 7.4's string2ull first tries a canonical signed decimal, then falls
/// back to strtoull. In particular, cursors differ from COUNT: a leading '+',
/// leading zeros/space and C-string termination are accepted by that fallback.
fn parse_cursor(value: &[u8]) -> Option<u64> {
    let value = &value[..value
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(value.len())];
    if let Some(integer) = parse_canonical_i64(value) {
        return u64::try_from(integer).ok();
    }
    let start = value
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))?;
    let mut digits = &value[start..];
    let negative = digits[0] == b'-';
    if matches!(digits[0], b'+' | b'-') {
        digits = &digits[1..];
    }
    if digits.is_empty() {
        return None;
    }
    let mut result = 0u64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        result = result.checked_mul(10)?.checked_add((digit - b'0') as u64)?;
    }
    Some(if negative {
        result.wrapping_neg()
    } else {
        result
    })
}

impl ReadRaftCommand for HScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HScan(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for HScanCommand {
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
    use crate::raft::types::core::structure::scan_map::ScanMap;
    use parking_lot::Mutex;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;

    fn parse(args: &[&[u8]]) -> Result<HScanParams, ProtocolError> {
        HScanCommand::parse_args(
            &args
                .iter()
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg))))
                .collect::<Vec<_>>(),
        )
    }

    fn snapshot(map: ScanMap<Bytes, HashValue>) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
            expire_at: None,
        }
    }

    fn page(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut outer)) = reply else {
            panic!("expected scan array");
        };
        assert_eq!(outer.len(), 2);
        let Value::Array(Some(values)) = outer.pop().unwrap() else {
            panic!("expected fields array");
        };
        let cursor = outer.pop().unwrap().string_bytes_clone().unwrap();
        let cursor = std::str::from_utf8(&cursor).unwrap().parse().unwrap();
        (
            cursor,
            values
                .into_iter()
                .map(|value| value.string_bytes_clone().unwrap())
                .collect(),
        )
    }

    #[test]
    fn cursor_grammar_matches_redis_unsigned_parser() {
        for (input, expected) in [
            (b"0".as_slice(), 0),
            (b"01", 1),
            (b"+1", 1),
            (b" 1", 1),
            (b"1\0junk", 1),
            (b"-0", 0),
            (b"-01", u64::MAX),
            (b"18446744073709551615", u64::MAX),
        ] {
            assert_eq!(parse_cursor(input), Some(expected), "{input:?}");
        }
        for input in [
            b"".as_slice(),
            b"-1",
            b"1 ",
            b"0x1",
            b"1.0",
            b"+",
            b"18446744073709551616",
            b"\xff",
        ] {
            assert_eq!(parse_cursor(input), None, "{input:?}");
            assert_eq!(
                parse(&[b"HSCAN", b"h", input]).unwrap_err(),
                ProtocolError::InvalidArgument("cursor")
            );
        }
        assert_eq!(
            parse(&[b"HSCAN", b"h"]).unwrap_err(),
            ProtocolError::WrongArgCount("hscan")
        );
    }

    #[test]
    fn options_are_case_insensitive_repeatable_and_validate_after_lookup() {
        let params = parse(&[
            b"HSCAN",
            b"h",
            b"0",
            b"count",
            b"1",
            b"MATCH",
            b"a*",
            b"COUNT",
            b"2",
            b"match",
            b"*",
            b"NoValues",
        ])
        .unwrap();
        assert_eq!(params.count, 2);
        assert!(params.pattern.is_none());
        assert!(params.no_values);
        for (options, error) in [
            (vec![b"COUNT".as_slice(), b"0"], ProtocolError::SyntaxError),
            (vec![b"COUNT".as_slice(), b"-1"], ProtocolError::SyntaxError),
            (
                vec![b"COUNT".as_slice(), b"01"],
                ProtocolError::NotAnInteger,
            ),
            (
                vec![b"COUNT".as_slice(), b"9223372036854775808"],
                ProtocolError::NotAnInteger,
            ),
            (vec![b"COUNT".as_slice()], ProtocolError::SyntaxError),
            (vec![b"MATCH".as_slice()], ProtocolError::SyntaxError),
            (
                vec![b"TYPE".as_slice(), b"hash"],
                ProtocolError::SyntaxError,
            ),
        ] {
            let mut args = vec![b"HSCAN".as_slice(), b"h", b"0"];
            args.extend(options);
            let params = parse(&args).unwrap();
            assert_eq!(page(params.execute(None)), (0, Vec::new()));
            let wrong_type = EntrySnapshot {
                value: MyValue::new(ValueObject::Int(1)),
                expire_at: None,
            };
            assert_eq!(
                params.execute(Some(wrong_type)).encode_proto(2),
                Value::from(ProtocolError::WrongType).encode_proto(2)
            );
            assert_eq!(
                params
                    .execute(Some(snapshot(ScanMap::new())))
                    .encode_proto(2),
                Value::from(error).encode_proto(2)
            );
            let restored: HScanParams =
                bincode2::deserialize(&bincode2::serialize(&params).unwrap()).unwrap();
            assert_eq!(restored.option_error, params.option_error);
        }
    }

    #[test]
    fn full_scan_returns_binary_fields_and_values_in_flat_resp_arrays() {
        let mut expected = BTreeMap::new();
        let mut map = ScanMap::new();
        for i in 0..257 {
            let field = Bytes::from(format!("field:{i}"));
            map.insert(field.clone(), HashValue::Int(i));
            expected.insert(field, Bytes::from(i.to_string()));
        }
        map.insert(
            Bytes::from_static(b"\0\xff"),
            HashValue::Str(Bytes::from_static(b"value\0\xff")),
        );
        expected.insert(
            Bytes::from_static(b"\0\xff"),
            Bytes::from_static(b"value\0\xff"),
        );
        map.insert(Bytes::new(), HashValue::Str(Bytes::new()));
        expected.insert(Bytes::new(), Bytes::new());
        let entry = snapshot(map);
        let mut params = parse(&[b"HSCAN", b"h", b"0", b"COUNT", b"3"]).unwrap();
        let mut actual = BTreeMap::new();
        let mut calls = 0;
        loop {
            let reply = params.execute(Some(entry.clone()));
            assert_eq!(reply.encode_proto(2), reply.encode_proto(3));
            let (cursor, fields) = page(reply);
            assert_eq!(fields.len() % 2, 0);
            for pair in fields.chunks_exact(2) {
                assert!(actual.insert(pair[0].clone(), pair[1].clone()).is_none());
            }
            calls += 1;
            assert!(calls <= 1024);
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert!(calls > 1);
        assert_eq!(actual, expected);
    }

    #[test]
    fn match_filters_candidates_without_forcing_a_full_scan() {
        let map = (0..256)
            .map(|i| (Bytes::from(format!("field:{i}")), HashValue::Int(i)))
            .collect();
        let entry = snapshot(map);
        let params =
            parse(&[b"HSCAN", b"h", b"0", b"MATCH", b"missing:*", b"COUNT", b"1"]).unwrap();
        let (cursor, fields) = page(params.execute(Some(entry.clone())));
        assert_ne!(cursor, 0, "MATCH must not run to completion to fill COUNT");
        assert!(fields.is_empty());
        let mut params = parse(&[
            b"HSCAN",
            b"h",
            b"0",
            b"MATCH",
            b"field:2??",
            b"NOVALUES",
            b"COUNT",
            b"5",
        ])
        .unwrap();
        let mut actual = HashSet::new();
        for _ in 0..1024 {
            let (cursor, fields) = page(params.execute(Some(entry.clone())));
            actual.extend(fields);
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0);
        assert_eq!(
            actual,
            (200..256)
                .map(|i| Bytes::from(format!("field:{i}")))
                .collect()
        );
    }

    #[test]
    fn replicated_scan_ignores_local_layout_and_accepts_an_ordinary_cursor() {
        let mut first = ScanMap::new();
        let mut second = ScanMap::with_capacity(4096);
        for i in 0..128 {
            first.insert(Bytes::from(format!("field:{i}")), HashValue::Int(i));
        }
        for i in (0..128).rev() {
            second.insert(Bytes::from(format!("field:{i}")), HashValue::Int(i));
        }
        let first = snapshot(first);
        let second = snapshot(second);
        let mut params = parse(&[b"HSCAN", b"h", b"0", b"COUNT", b"1"]).unwrap();
        params.cursor = page(params.execute(Some(first.clone()))).0;
        assert_ne!(params.cursor, 0);
        let first_reply = params.execute_with_clock(Some(first), 100);
        let second_reply = params.execute_with_clock(Some(second), 100);
        assert_eq!(first_reply.encode_proto(2), second_reply.encode_proto(2));
        let (cursor, fields) = page(first_reply);
        assert_eq!(cursor, 0);
        assert_eq!(fields.len(), 256);
        assert!(fields.chunks_exact(2).map(|pair| &pair[0]).is_sorted());
    }

    #[test]
    fn empty_scan_and_novalues_preserve_redis_reply_shape() {
        let params = parse(&[b"HSCAN", b"h", b"18446744073709551615"]).unwrap();
        assert_eq!(
            params.execute(None).encode_proto(2).as_slice(),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );
        let mut map = ScanMap::new();
        map.insert(Bytes::new(), HashValue::Int(42));
        map.insert(Bytes::from_static(b"a"), HashValue::Int(-1));
        let params = parse(&[b"HSCAN", b"h", b"0", b"NOVALUES", b"MATCH", b"*"]).unwrap();
        let reply = params.execute_with_clock(Some(snapshot(map)), 0);
        assert_eq!(
            reply.encode_proto(3).as_slice(),
            b"*2\r\n$1\r\n0\r\n*2\r\n$0\r\n\r\n$1\r\na\r\n"
        );
    }

    #[test]
    fn cache_reads_hide_expired_hashes_without_deleting_them() {
        use crate::raft::types::core::mocha::core::MyCache;
        use crate::raft::types::core::mocha::request_handler::read_request;

        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let hash = Arc::new(Mutex::new(ScanMap::from([(
            Bytes::from_static(b"field"),
            HashValue::Int(7),
        )])));
        let storage = &cache.databases[0].mocha;
        storage.insert_absolute(
            Bytes::from_static(b"h"),
            MyValue::new(ValueObject::Hash(hash.clone())),
            200,
        );
        let params = parse(&[b"HSCAN", b"h", b"0"]).unwrap();
        assert_eq!(
            page(read_request(
                &cache,
                ReadOperation::HScan(params.clone()),
                0,
                199
            )),
            (
                0,
                vec![Bytes::from_static(b"field"), Bytes::from_static(b"7")]
            )
        );
        for clock in [200, 201] {
            assert_eq!(
                page(read_request(
                    &cache,
                    ReadOperation::HScan(params.clone()),
                    0,
                    clock
                )),
                (0, Vec::new())
            );
            assert_eq!(
                page(cache.execute_read(params.clone(), 0, clock)),
                (0, Vec::new())
            );
        }
        assert_eq!(
            storage.len(),
            1,
            "read-clock filtering must not delete keys"
        );
        assert_eq!(hash.lock().len(), 1);
        assert!(
            storage
                .get_with_read_clock(b"h".as_slice(), Some(199))
                .is_some()
        );
    }

    #[test]
    fn real_hash_writes_between_pages_do_not_lose_continuously_present_fields() {
        use crate::protocol::hash::hdel::HDelReq;
        use crate::protocol::hash::hset::HSetReq;
        use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
        use crate::raft::types::core::mocha::request_handler::read_request;

        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: cache.set_write_clock(100),
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let stable: HashSet<_> = (0..128)
            .map(|i| Bytes::from(format!("stable:{i}")))
            .collect();
        cache.execute_compute(
            HSetReq {
                key: "h".into(),
                elements: stable
                    .iter()
                    .map(|field| (field.clone(), Bytes::from_static(b"value")))
                    .collect(),
            },
            &mut update,
        );
        let mut params = parse(&[b"HSCAN", b"h", b"0", b"COUNT", b"1", b"NOVALUES"]).unwrap();
        let mut seen = HashSet::new();
        let temporary: Vec<_> = (0..1024)
            .map(|i| Bytes::from(format!("temporary:{i}")))
            .collect();
        for step in 0..4096 {
            let (cursor, fields) = page(read_request(
                &cache,
                ReadOperation::HScan(params.clone()),
                0,
                100,
            ));
            seen.extend(fields);
            if step == 0 {
                assert_ne!(cursor, 0);
                // Force several synchronous expansions after a cursor is live.
                cache.execute_compute(
                    HSetReq {
                        key: "h".into(),
                        elements: temporary
                            .iter()
                            .map(|field| (field.clone(), Bytes::from_static(b"temporary")))
                            .collect(),
                    },
                    &mut update,
                );
            } else if step <= temporary.len() {
                cache.execute_compute(
                    HDelReq {
                        key: "h".into(),
                        fields: vec![temporary[step - 1].clone()],
                    },
                    &mut update,
                );
            }
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0, "bounded collection scan must terminate");
        assert!(
            stable.is_subset(&seen),
            "all continuously present fields must be returned"
        );
        assert!(
            seen.iter()
                .all(|field| stable.contains(field) || temporary.contains(field))
        );
    }
}
