//! SCAN command implementation
//!
//! SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
//!
//! Reply: array of the next cursor and the keys of this page.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::key::type_::value_object_to_type_string;
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::scan_cursor::{self, ScanOptions};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for SCAN command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanParams {
    pub cursor: u64,
    pub count: usize,
    /// None means every key matches, so no glob matcher is built.
    pub pattern: Option<Bytes>,
    pub type_filter: Option<Bytes>,
}

impl Display for ScanParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SCAN {} COUNT {}", self.cursor, self.count)
    }
}

impl ScanParams {
    /// Page for a replica-local read.
    pub fn execute(&self, cache: &MyCache, db_number: u16, read_clock: u64) -> Value {
        self.page(cache, db_number, read_clock, false)
    }

    /// Page for a replicated read. Every replica rebuilds the same key order,
    /// so EXEC and Lua produce identical pages on all nodes.
    pub fn execute_with_clock(&self, cache: &MyCache, db_number: u16, read_clock: u64) -> Value {
        self.page(cache, db_number, read_clock, true)
    }

    fn page(&self, cache: &MyCache, db_number: u16, read_clock: u64, rebuild: bool) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        let matcher = self
            .pattern
            .as_ref()
            .map(|pattern| GlobMatcher::new(pattern));
        let (cursor, keys) = database.mocha.scan(
            self.cursor,
            Some(read_clock),
            self.count,
            rebuild,
            |key, value| {
                matcher
                    .as_ref()
                    .is_none_or(|matcher| matcher.matches(key.as_ref()))
                    && self.type_filter.as_ref().is_none_or(|filter| {
                        value_object_to_type_string(&value.data)
                            .as_bytes()
                            .eq_ignore_ascii_case(filter)
                    })
            },
        );
        scan_cursor::reply(
            cursor,
            keys.into_iter()
                .map(|key| Value::BulkString(Some(key)))
                .collect(),
        )
    }

    /// Parse `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`.
    ///
    /// Options are case insensitive, may repeat except TYPE, and each needs a
    /// value, matching the option loop of Redis.
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("scan"));
        }
        let cursor = scan_cursor::parse_cursor(&items[1])?;

        let mut type_filter = None;
        let options =
            ScanOptions::parse(cursor, items.get(2..).unwrap_or_default(), |name, rest| {
                if !option_eq(name, b"type") {
                    return Ok(None);
                }
                // A repeated TYPE overwrites the previous one, like Redis.
                let value = rest
                    .first()
                    .and_then(Value::string_bytes_clone)
                    .ok_or(ProtocolError::SyntaxError)?;
                type_filter = Some(value);
                Ok(Some(1))
            })?;

        Ok(Self {
            cursor: options.cursor,
            count: options.count,
            pattern: options.pattern,
            type_filter,
        })
    }
}

/// SCAN command executor
pub struct ScanCommand;

impl ReadRaftCommand for ScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Scan(ScanParams::parse(items)?))
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
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::ValueObject;
    use parking_lot::Mutex;
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Arc;

    fn request(parts: &[&str]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(part.as_bytes().to_vec().into())))
            .collect()
    }

    fn params(parts: &[&str]) -> Result<ScanParams, ProtocolError> {
        ScanParams::parse(&request(parts))
    }

    fn error_text(parts: &[&str]) -> String {
        match ScanCommand.read_operation(&request(parts)) {
            Ok(_) => panic!("expected an error for {parts:?}"),
            Err(error) => error.to_string(),
        }
    }

    fn scan(cursor: u64, count: usize) -> ScanParams {
        ScanParams {
            cursor,
            count,
            pattern: None,
            type_filter: None,
        }
    }

    fn unpack(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut items)) = reply else {
            panic!("expected a two element SCAN reply: {reply:?}");
        };
        let Some(Value::Array(Some(keys))) = items.pop() else {
            panic!("expected a key array: {items:?}");
        };
        let Some(Value::BulkString(Some(cursor))) = items.pop() else {
            panic!("expected a bulk string cursor: {items:?}");
        };
        (
            std::str::from_utf8(&cursor).unwrap().parse().unwrap(),
            keys.into_iter()
                .map(|key| match key {
                    Value::BulkString(Some(key)) => key,
                    other => panic!("expected a bulk string key: {other:?}"),
                })
                .collect(),
        )
    }

    fn page(
        cache: &MyCache,
        base: &ScanParams,
        cursor: u64,
        read_clock: u64,
        replicated: bool,
    ) -> (u64, Vec<Bytes>) {
        let params = ScanParams {
            cursor,
            ..base.clone()
        };
        let reply = if replicated {
            params.execute_with_clock(cache, 0, read_clock)
        } else {
            params.execute(cache, 0, read_clock)
        };
        unpack(reply)
    }

    fn scan_all(
        cache: &MyCache,
        base: &ScanParams,
        read_clock: u64,
        replicated: bool,
    ) -> Vec<Bytes> {
        let mut cursor = 0;
        let mut collected = Vec::new();
        let mut pages = 0;
        loop {
            let (next, keys) = page(cache, base, cursor, read_clock, replicated);
            collected.extend(keys);
            pages += 1;
            assert!(pages < 10_000, "the cursor never reached the end");
            if next == 0 {
                return collected;
            }
            assert_ne!(next, cursor, "the cursor must advance");
            cursor = next;
        }
    }

    fn seeded(order: impl Iterator<Item = &'static str>) -> MyCache {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        for key in order {
            cache.databases[0]
                .mocha
                .insert_persistent(key.into(), MyValue::new(ValueObject::String("v".into())));
        }
        cache
    }

    #[test]
    fn full_iteration_returns_every_visible_key_once() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let storage = &cache.databases[0].mocha;
        for index in 0u16..40 {
            storage.insert_persistent(
                format!("key:{index}").into(),
                MyValue::new(ValueObject::String("v".into())),
            );
        }
        storage.insert_absolute(
            "key:gone".into(),
            MyValue::new(ValueObject::String("v".into())),
            90,
        );
        storage.insert_absolute(
            "key:live".into(),
            MyValue::new(ValueObject::String("v".into())),
            1_000,
        );

        let collected = scan_all(&cache, &scan(0, 7), 200, false);
        let mut expected: Vec<Bytes> = (0u16..40)
            .map(|index| format!("key:{index}").into())
            .collect();
        expected.push("key:live".into());
        let unique: HashSet<_> = collected.iter().collect();
        assert_eq!(unique.len(), collected.len(), "a key was returned twice");
        assert_eq!(collected.len(), expected.len());
        for key in &expected {
            assert!(unique.contains(key), "missing {key:?}");
        }
        assert!(!unique.contains(&Bytes::from_static(b"key:gone")));
        assert_eq!(storage.len(), 42, "scanning must not delete entries");
    }

    #[test]
    fn match_and_type_filters_select_the_same_keys_as_redis() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let storage = &cache.databases[0].mocha;
        let entries = [
            ("str:a", ValueObject::String("v".into())),
            ("str:b", ValueObject::Int(1)),
            (
                "list",
                ValueObject::List(Arc::new(Mutex::new(VecDeque::new()))),
            ),
            (
                "hash",
                ValueObject::Hash(Arc::new(Mutex::new(HashMap::new()))),
            ),
            (
                "set",
                ValueObject::Set(Arc::new(Mutex::new(HashSet::new()))),
            ),
            (
                "zset",
                ValueObject::ZSet(Arc::new(Mutex::new(Default::default()))),
            ),
        ];
        for (key, value) in entries {
            storage.insert_persistent(key.into(), MyValue::new(value));
        }

        let mut matched = scan_all(
            &cache,
            &params(&["SCAN", "0", "MATCH", "str:*"]).unwrap(),
            200,
            false,
        );
        matched.sort();
        assert_eq!(
            matched,
            vec![Bytes::from_static(b"str:a"), Bytes::from_static(b"str:b")]
        );

        // Redis compares the type name case insensitively.
        let mut strings = scan_all(
            &cache,
            &params(&["SCAN", "0", "COUNT", "1", "TYPE", "STRING"]).unwrap(),
            200,
            false,
        );
        strings.sort();
        assert_eq!(
            strings,
            vec![Bytes::from_static(b"str:a"), Bytes::from_static(b"str:b")]
        );

        let only_list = scan_all(
            &cache,
            &params(&["SCAN", "0", "TYPE", "list"]).unwrap(),
            200,
            false,
        );
        assert_eq!(only_list, vec![Bytes::from_static(b"list")]);

        let mut both = scan_all(
            &cache,
            &params(&["SCAN", "0", "MATCH", "str:*", "TYPE", "string"]).unwrap(),
            200,
            false,
        );
        both.sort();
        assert_eq!(
            both,
            vec![Bytes::from_static(b"str:a"), Bytes::from_static(b"str:b")]
        );

        // An unknown type name matches nothing instead of failing.
        let (cursor, keys) = page(
            &cache,
            &params(&["SCAN", "0", "TYPE", "nosuchtype"]).unwrap(),
            0,
            200,
            false,
        );
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn invalid_arguments_match_redis_errors() {
        assert!(matches!(
            params(&["SCAN"]),
            Err(ProtocolError::WrongArgCount("scan"))
        ));
        assert_eq!(error_text(&["SCAN", "x"]), "ERR invalid cursor");
        assert_eq!(
            error_text(&["SCAN", "18446744073709551616"]),
            "ERR invalid cursor"
        );
        assert_eq!(error_text(&["SCAN", "0", "COUNT"]), "ERR syntax error");
        assert_eq!(error_text(&["SCAN", "0", "COUNT", "0"]), "ERR syntax error");
        assert_eq!(
            error_text(&["SCAN", "0", "COUNT", "-3"]),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&["SCAN", "0", "COUNT", "abc"]),
            "ERR value is not an integer or out of range"
        );
        // A repeated TYPE overwrites the previous one, like Redis.
        assert_eq!(
            params(&["SCAN", "0", "TYPE", "string", "TYPE", "list"])
                .unwrap()
                .type_filter,
            Some(Bytes::from_static(b"list"))
        );
        // Option names compare like C strings, so a NUL terminator ends one.
        assert_eq!(
            params(&["SCAN", "0", "TYPE\0", "hash"])
                .unwrap()
                .type_filter,
            Some(Bytes::from_static(b"hash"))
        );
        assert_eq!(error_text(&["SCAN", "0", "BOGUS", "1"]), "ERR syntax error");
        let parsed = params(&[
            "SCAN", "9", "COUNT", "3", "MATCH", "a*", "TYPE", "hash", "count", "4",
        ])
        .unwrap();
        assert_eq!(parsed.cursor, 9);
        assert_eq!(parsed.count, 4);
        assert_eq!(parsed.pattern, Some(Bytes::from_static(b"a*")));
        assert_eq!(parsed.type_filter, Some(Bytes::from_static(b"hash")));
        // MATCH may repeat and the last one wins, while a bare "*" disables it.
        assert_eq!(
            params(&["SCAN", "0", "MATCH", "a*", "MATCH", "*"])
                .unwrap()
                .pattern,
            None
        );
    }

    #[test]
    fn replicas_paginate_identically_and_share_cursors() {
        let names = [
            "key:0", "key:1", "key:2", "key:3", "key:4", "key:5", "key:6", "key:7", "key:8",
            "key:9", "other:0", "other:1",
        ];
        let forward = seeded(names.iter().copied());
        let reverse = seeded(names.iter().rev().copied());
        let base = params(&["SCAN", "0", "COUNT", "3", "MATCH", "key:*"]).unwrap();

        let left = scan_all(&forward, &base, 200, true);
        let right = scan_all(&reverse, &base, 200, true);
        assert_eq!(left, right, "replicas must return the same pages");
        assert_eq!(left.len(), 10);

        // A cursor issued by one node resumes on another node.
        let (cursor, first) = page(&forward, &base, 0, 200, false);
        assert_ne!(cursor, 0);
        assert!(!first.is_empty());
        assert_eq!(
            page(&forward, &base, cursor, 200, false),
            page(&reverse, &base, cursor, 200, false)
        );
    }

    #[test]
    fn reply_encodes_like_redis_in_both_protocols() {
        let cache = seeded(["only"].into_iter());
        let reply = scan(0, 10).execute(&cache, 0, 200);
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto),
                b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"
            );
        }
    }

    #[test]
    fn keys_deleted_between_pages_disappear() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let storage = &cache.databases[0].mocha;
        let keys: Vec<Bytes> = (0u8..6).map(|index| vec![b'k', index].into()).collect();
        for key in &keys {
            storage.insert_persistent(key.clone(), MyValue::new(ValueObject::String("v".into())));
        }

        let base = scan(0, 1);
        let (mut cursor, first) = page(&cache, &base, 0, 200, false);
        assert_eq!(first.len(), 1);
        assert_ne!(cursor, 0);
        for key in keys.iter().filter(|key| !first.contains(key)) {
            assert!(storage.remove(key).is_some());
        }

        let mut collected = first.clone();
        while cursor != 0 {
            let (next, keys) = page(&cache, &base, cursor, 200, false);
            assert_eq!(next, 0, "deleted keys must not extend the iteration");
            collected.extend(keys);
            cursor = next;
        }
        assert_eq!(collected, first);

        storage.clear();
        let (cursor, keys) = page(&cache, &base, 0, 200, false);
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
        storage.insert_persistent(
            Bytes::from_static(b"k0"),
            MyValue::new(ValueObject::String("v".into())),
        );
        let collected = scan_all(&cache, &base, 200, false);
        assert_eq!(collected, vec![Bytes::from_static(b"k0")]);
    }
}
