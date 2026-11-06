//! SCAN cursor [MATCH pattern] [COUNT count] [TYPE type].

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, Mocha};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::scan::parse_cursor;
use crate::protocol::key::type_::value_object_to_type_string;
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanParams {
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    pub type_filter: Option<Bytes>,
}

impl Display for ScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SCAN {} COUNT {}", self.cursor, self.count)?;
        if let Some(pattern) = &self.pattern {
            write!(f, " MATCH {}", String::from_utf8_lossy(pattern))?;
        }
        if let Some(kind) = &self.type_filter {
            write!(f, " TYPE {}", String::from_utf8_lossy(kind))?;
        }
        Ok(())
    }
}

impl MultiReadCommand for ScanParams {
    fn keys(&self) -> &Vec<Bytes> {
        // A database-wide command has no fixed input key list. The database
        // hook below visits candidates without materializing all keys/values.
        static KEYS: Vec<Bytes> = Vec::new();
        &KEYS
    }

    fn execute(&self, _values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        Value::error("ERR SCAN requires database access")
    }

    fn execute_on_database(
        &self,
        cache: &Mocha<Bytes, MyValue>,
        read_clock: u64,
        deterministic: bool,
    ) -> Value {
        let matcher = self.pattern.as_deref().map(GlobMatcher::new);
        let mut keys = Vec::with_capacity(self.count.min(256));
        let mut visit = |key: &Bytes, value: &MyValue| {
            if matcher.as_ref().is_none_or(|pattern| pattern.matches(key))
                && self.type_filter.as_ref().is_none_or(|kind| {
                    kind.eq_ignore_ascii_case(value_object_to_type_string(&value.data).as_bytes())
                })
            {
                keys.push(key.clone());
            }
        };
        let cursor = if deterministic {
            // Lua/EXEC may use the returned keys in subsequent writes. Return
            // every matching key in byte order, independent of replica-local
            // hash seeds and capacity, even for an ordinary scan's cursor.
            cache.visit_entries(Some(read_clock), &mut visit);
            keys.sort_unstable();
            0
        } else {
            cache.scan(self.cursor, self.count, Some(read_clock), visit)
        };
        Self::reply(cursor, keys)
    }
}

impl ScanParams {
    fn reply(cursor: u64, keys: Vec<Bytes>) -> Value {
        Value::Array(Some(vec![
            Value::BulkString(Some(cursor.to_string().into())),
            Value::Array(Some(
                keys.into_iter()
                    .map(|key| Value::BulkString(Some(key)))
                    .collect(),
            )),
        ]))
    }
}

pub struct ScanCommand;

impl ScanCommand {
    pub(crate) fn parse_args(items: &[Value]) -> Result<ScanParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("scan"));
        }
        let cursor = items[1]
            .string_bytes_clone()
            .and_then(|value| parse_cursor(&value))
            .ok_or(ProtocolError::InvalidArgument("cursor"))?;
        let mut params = ScanParams {
            cursor,
            count: 10,
            pattern: None,
            type_filter: None,
        };
        let mut index = 2;
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
            } else if option_eq(&option, b"TYPE") && index + 1 < items.len() {
                let kind = items[index + 1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::SyntaxError)?;
                // Redis compares type names as C strings. Unknown names are
                // valid filters and simply do not match any stored value.
                let end = kind
                    .iter()
                    .position(|&byte| byte == 0)
                    .unwrap_or(kind.len());
                params.type_filter = Some(kind.slice(..end));
                index += 2;
            } else if option_eq(&option, b"NOVALUES") {
                return Err(ProtocolError::response(
                    "ERR NOVALUES option can only be used in HSCAN",
                ));
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        Ok(params)
    }
}

impl ReadRaftCommand for ScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Scan(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ScanCommand {
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
            .multi_read(self.read_operation(items)?, client.db_number)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::bloom_filter::BloomObject;
    use crate::raft::types::core::mocha::core::MyCache;
    use crate::raft::types::core::mocha::request_handler::read_request;
    use crate::raft::types::core::structure::scan_map::ScanMap;
    use crate::raft::types::core::structure::scan_set::ScanSet;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use crate::raft::types::core::structure::stream::RedisStream;
    use crate::raft::types::core::value_object::ValueObject;
    use parking_lot::{Mutex, RwLock};
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::Arc;

    fn parse(args: &[&[u8]]) -> Result<ScanParams, ProtocolError> {
        ScanCommand::parse_args(
            &args
                .iter()
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg))))
                .collect::<Vec<_>>(),
        )
    }

    fn page(value: Value) -> (u64, Vec<Bytes>) {
        assert_eq!(value.encode_proto(2), value.encode_proto(3));
        let Value::Array(Some(mut reply)) = value else {
            panic!("SCAN must return a two-element array");
        };
        assert_eq!(reply.len(), 2);
        let Value::Array(Some(keys)) = reply.pop().unwrap() else {
            panic!("SCAN keys must be an array");
        };
        let cursor = reply.pop().unwrap().string_bytes_clone().unwrap();
        (
            parse_cursor(&cursor).unwrap(),
            keys.into_iter()
                .map(|key| match key {
                    Value::BulkString(Some(key)) => key,
                    other => panic!("SCAN keys must be bulk strings: {other:?}"),
                })
                .collect(),
        )
    }

    fn scan_all(cache: &MyCache, mut params: ScanParams, db: u16, clock: u64) -> BTreeSet<Bytes> {
        let mut keys = BTreeSet::new();
        for _ in 0..16384 {
            let (cursor, batch) = page(read_request(
                cache,
                ReadOperation::Scan(params.clone()),
                db,
                clock,
            ));
            keys.extend(batch);
            if cursor == 0 {
                return keys;
            }
            params.cursor = cursor;
            // COUNT can change between calls without changing cursor meaning.
            params.count = params.count % 13 + 1;
        }
        panic!("bounded SCAN must terminate");
    }

    #[test]
    fn parses_redis_cursors_and_rejects_invalid_options_even_on_empty_database() {
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
            assert_eq!(parse(&[b"SCAN", input]).unwrap().cursor, expected);
        }
        for input in [
            b"".as_slice(),
            b"-1",
            b"1 ",
            b"1.0",
            b"18446744073709551616",
            b"\xff",
        ] {
            assert_eq!(
                parse(&[b"SCAN", input]).unwrap_err(),
                ProtocolError::InvalidArgument("cursor")
            );
        }
        assert_eq!(
            parse(&[b"SCAN"]).unwrap_err(),
            ProtocolError::WrongArgCount("scan")
        );
        for (options, expected) in [
            (vec![b"COUNT".as_slice(), b"0"], ProtocolError::SyntaxError),
            (vec![b"COUNT".as_slice(), b"-1"], ProtocolError::SyntaxError),
            (
                vec![b"COUNT".as_slice(), b"01"],
                ProtocolError::NotAnInteger,
            ),
            (
                vec![b"COUNT".as_slice(), b"+1"],
                ProtocolError::NotAnInteger,
            ),
            (
                vec![b"COUNT".as_slice(), b"9223372036854775808"],
                ProtocolError::NotAnInteger,
            ),
            (vec![b"COUNT".as_slice()], ProtocolError::SyntaxError),
            (vec![b"MATCH".as_slice()], ProtocolError::SyntaxError),
            (vec![b"TYPE".as_slice()], ProtocolError::SyntaxError),
            (vec![b"other".as_slice()], ProtocolError::SyntaxError),
            (
                vec![b"NOVALUES".as_slice()],
                ProtocolError::response("ERR NOVALUES option can only be used in HSCAN"),
            ),
        ] {
            let mut args = vec![b"SCAN".as_slice(), b"0"];
            args.extend(options);
            assert_eq!(parse(&args).unwrap_err(), expected);
        }
        let params = parse(&[
            b"SCAN",
            b"0",
            b"COUNT\0ignored",
            b"1",
            b"match",
            b"a*",
            b"count",
            b"2",
            b"MATCH",
            b"*",
            b"TYPE",
            b"set",
            b"type",
            b"STRING\0suffix",
        ])
        .unwrap();
        assert_eq!(params.count, 2);
        assert!(params.pattern.is_none());
        assert_eq!(params.type_filter.as_deref(), Some(b"STRING".as_slice()));
        let restored: ScanParams =
            bincode2::deserialize(&bincode2::serialize(&params).unwrap()).unwrap();
        assert_eq!(restored.to_string(), params.to_string());
    }

    #[test]
    fn scans_binary_keys_in_selected_database_and_preserves_resp_shape() {
        let cache = MyCache::new(2).unwrap();
        let mut expected: BTreeSet<Bytes> = (0..513).map(|i| format!("key:{i}").into()).collect();
        expected.extend([Bytes::new(), Bytes::from_static(b"\0\xff")]);
        for key in &expected {
            cache.databases[1]
                .mocha
                .insert_persistent(key.clone(), MyValue::new(ValueObject::Int(1)));
        }
        cache.databases[0]
            .mocha
            .insert_persistent("other-db".into(), MyValue::new(ValueObject::Int(2)));
        assert_eq!(
            scan_all(
                &cache,
                parse(&[b"SCAN", b"0", b"COUNT", b"1"]).unwrap(),
                1,
                0
            ),
            expected
        );
        let params = parse(&[b"SCAN", b"0"]).unwrap();
        assert_eq!(
            scan_all(&cache, params.clone(), 0, 0),
            BTreeSet::from(["other-db".into()])
        );
        assert!(matches!(
            read_request(&cache, ReadOperation::Scan(params), 2, 0),
            Value::Error(_)
        ));
        let empty = MyCache::new(1).unwrap();
        let reply = read_request(
            &empty,
            ReadOperation::Scan(parse(&[b"SCAN", b"18446744073709551615"]).unwrap()),
            0,
            0,
        );
        assert_eq!(reply.encode(), b"*2\r\n$1\r\n0\r\n*0\r\n");
    }

    #[test]
    fn filtering_does_not_fill_count_by_scanning_the_whole_database() {
        let cache = MyCache::new(1).unwrap();
        for i in 0..512 {
            cache.databases[0]
                .mocha
                .insert_persistent(format!("key:{i}").into(), MyValue::new(ValueObject::Int(i)));
        }
        for options in [
            vec![b"MATCH".as_slice(), b"missing:*"],
            vec![b"TYPE".as_slice(), b"unknown"],
        ] {
            let mut args = vec![b"SCAN".as_slice(), b"0", b"COUNT", b"1"];
            args.extend(options);
            let (cursor, keys) = page(read_request(
                &cache,
                ReadOperation::Scan(parse(&args).unwrap()),
                0,
                0,
            ));
            assert_ne!(
                cursor, 0,
                "filters must run after COUNT candidates are selected"
            );
            assert!(keys.is_empty());
        }
        let actual = scan_all(
            &cache,
            parse(&[b"SCAN", b"0", b"MATCH", b"key:2??", b"COUNT", b"3"]).unwrap(),
            0,
            0,
        );
        assert_eq!(
            actual,
            (200..300).map(|i| format!("key:{i}").into()).collect()
        );
    }

    #[test]
    fn type_filters_all_stored_types_and_uses_type_command_names() {
        let cache = MyCache::new(1).unwrap();
        let values = [
            ("string", ValueObject::String("value".into())),
            ("integer", ValueObject::Int(42)),
            (
                "list",
                ValueObject::List(Arc::new(Mutex::new(VecDeque::new()))),
            ),
            (
                "hash",
                ValueObject::Hash(Arc::new(Mutex::new(ScanMap::new()))),
            ),
            (
                "set",
                ValueObject::Set(Arc::new(Mutex::new(ScanSet::new()))),
            ),
            (
                "zset",
                ValueObject::ZSet(Arc::new(Mutex::new(SortedSet::new()))),
            ),
            (
                "bloom",
                ValueObject::Bloom(Arc::new(Mutex::new(
                    BloomObject::new(100, 0.01, 2, false).unwrap(),
                ))),
            ),
            (
                "stream",
                ValueObject::Stream(Arc::new(RwLock::new(RedisStream::default()))),
            ),
        ];
        for (key, value) in values {
            cache.databases[0]
                .mocha
                .insert_persistent(key.into(), MyValue::new(value));
        }
        for (kind, expected) in [
            ("StRiNg", vec!["integer", "string"]),
            ("list", vec!["list"]),
            ("hash", vec!["hash"]),
            ("set", vec!["set"]),
            ("zset", vec!["zset"]),
            ("MBbloom--", vec!["bloom"]),
            ("stream", vec!["stream"]),
            ("none", vec![]),
            ("", vec![]),
            ("unknown", vec![]),
        ] {
            let params = parse(&[b"SCAN", b"0", b"TYPE", kind.as_bytes(), b"COUNT", b"1"]).unwrap();
            let expected: BTreeSet<Bytes> = expected.into_iter().map(Bytes::from).collect();
            assert_eq!(scan_all(&cache, params.clone(), 0, 0), expected);
            assert_eq!(
                page(cache.execute_multi_read(params, 0, 0)).1,
                expected.into_iter().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn expiration_filters_both_clocks_without_deleting_keys() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let storage = &cache.databases[0].mocha;
        storage.insert_absolute("expired".into(), MyValue::new(ValueObject::Int(1)), 100);
        storage.insert_absolute("live".into(), MyValue::new(ValueObject::Int(1)), 200);
        storage.insert_persistent("persistent".into(), MyValue::new(ValueObject::Int(1)));
        let params = parse(&[b"SCAN", b"0"]).unwrap();
        for (clock, expected) in [
            (99, vec!["live", "persistent"]),
            (199, vec!["live", "persistent"]),
            (200, vec!["persistent"]),
        ] {
            let expected: BTreeSet<Bytes> = expected.into_iter().map(Bytes::from).collect();
            assert_eq!(scan_all(&cache, params.clone(), 0, clock), expected);
            assert_eq!(
                page(cache.execute_multi_read(params.clone(), 0, clock)).1,
                expected.into_iter().collect::<Vec<_>>()
            );
        }
        assert_eq!(storage.len(), 3, "SCAN only filters expired entries");
    }

    #[test]
    fn growth_and_deletion_between_pages_preserve_continuously_present_keys() {
        let cache = MyCache::new(1).unwrap();
        let storage = &cache.databases[0].mocha;
        let stable: BTreeSet<Bytes> = (0..128).map(|i| format!("stable:{i}").into()).collect();
        for key in &stable {
            storage.insert_persistent(key.clone(), MyValue::new(ValueObject::Int(1)));
        }
        let mut params = parse(&[b"SCAN", b"0", b"COUNT", b"1"]).unwrap();
        let mut seen = BTreeSet::new();
        for step in 0..16384 {
            let (cursor, batch) = page(read_request(
                &cache,
                ReadOperation::Scan(params.clone()),
                0,
                0,
            ));
            seen.extend(batch);
            if step == 0 {
                assert_ne!(cursor, 0);
                for i in 0..2048 {
                    storage.insert_persistent(
                        format!("temporary:{i}").into(),
                        MyValue::new(ValueObject::Int(i)),
                    );
                }
            } else if step <= 2048 {
                storage.remove(&Bytes::from(format!("temporary:{}", step - 1)));
                let key = stable.iter().nth(step % stable.len()).unwrap();
                storage.insert_persistent(key.clone(), MyValue::new(ValueObject::Int(step as i64)));
            }
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0);
        assert!(stable.is_subset(&seen));
    }
}
