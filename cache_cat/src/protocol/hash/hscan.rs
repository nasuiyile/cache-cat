//! HSCAN command implementation
//!
//! HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
//!
//! Reply: array of the next cursor and the fields of this page, each followed
//! by its value unless NOVALUES is given.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::scan_cursor::{self, ScanOptions};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HSCAN arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    pub no_values: bool,
}

impl Display for HScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HSCAN {} {} COUNT {}",
            String::from_utf8_lossy(&self.key),
            self.cursor,
            self.count
        )
    }
}

impl HScanParams {
    /// Read one page of the hash. A replicated read rebuilds the element order,
    /// so every replica returns the same fields for the same cursor.
    pub fn execute(
        &self,
        cache: &MyCache,
        db_number: u16,
        read_clock: u64,
        replicated: bool,
    ) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        let Some(entry) = database
            .mocha
            .get_with_read_clock(&self.key, Some(read_clock))
        else {
            return scan_cursor::reply(0, Vec::new());
        };
        let ValueObject::Hash(map) = entry.value.data else {
            return ProtocolError::WrongType.into();
        };

        let live_len = map.lock().len();
        // A new iteration must see every field that exists when it starts.
        let rebuild = replicated || self.cursor == 0;
        let index = database
            .collection_indexes
            .get(&self.key, live_len, rebuild, || {
                map.lock().keys().cloned().collect()
            });
        let guard = map.lock();
        let matcher = self
            .pattern
            .as_ref()
            .map(|pattern| GlobMatcher::new(pattern));
        let (cursor, fields) = index.page(self.cursor, self.count, |field| {
            // 索引可能包含建立之后被删除的字段
            guard.contains_key(field)
                && matcher
                    .as_ref()
                    .is_none_or(|matcher| matcher.matches(field.as_ref()))
        });

        let mut elements = Vec::with_capacity(if self.no_values {
            fields.len()
        } else {
            fields.len() * 2
        });
        for field in fields {
            let Some(value) = guard.get(&field) else {
                continue;
            };
            elements.push(Value::BulkString(Some(field)));
            if !self.no_values {
                elements.push(Value::BulkString(Some(value.to_bytes())));
            }
        }
        scan_cursor::reply(cursor, elements)
    }

    /// Parse `HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]`.
    fn parse_args(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hscan"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let cursor = scan_cursor::parse_cursor(&items[2])?;

        let mut no_values = false;
        let options = ScanOptions::parse(cursor, items.get(3..).unwrap_or_default(), |name, _| {
            if option_eq(name, b"novalues") {
                no_values = true;
                return Ok(Some(0));
            }
            Ok(None)
        })?;

        Ok(Self {
            key,
            cursor: options.cursor,
            count: options.count,
            pattern: options.pattern,
            no_values,
        })
    }
}

/// HSCAN command handler
pub struct HScanCommand;

impl ReadRaftCommand for HScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::HScan(HScanParams::parse_args(items)?))
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
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::HashValue;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn request(parts: &[&str]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(part.as_bytes().to_vec().into())))
            .collect()
    }

    fn params(parts: &[&str]) -> Result<HScanParams, ProtocolError> {
        HScanParams::parse_args(&request(parts))
    }

    fn error_text(parts: &[&str]) -> String {
        match HScanCommand.read_operation(&request(parts)) {
            Ok(_) => panic!("expected an error for {parts:?}"),
            Err(error) => error.to_string(),
        }
    }

    /// Cache holding one hash whose fields are inserted in the given order.
    fn seeded(fields: impl DoubleEndedIterator<Item = String>) -> MyCache {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let map: HashMap<Bytes, HashValue> = fields
            .map(|field| {
                let value = format!("value:{field}");
                (Bytes::from(field), HashValue::Str(Bytes::from(value)))
            })
            .collect();
        cache.databases[0].mocha.insert_persistent(
            "h".into(),
            MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
        );
        cache
    }

    fn unpack(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut items)) = reply else {
            panic!("expected a two element HSCAN reply: {reply:?}");
        };
        let Some(Value::Array(Some(elements))) = items.pop() else {
            panic!("expected an element array: {items:?}");
        };
        let Some(Value::BulkString(Some(cursor))) = items.pop() else {
            panic!("expected a bulk string cursor: {items:?}");
        };
        let elements = elements
            .into_iter()
            .map(|element| match element {
                Value::BulkString(Some(element)) => element,
                other => panic!("expected a bulk string element: {other:?}"),
            })
            .collect();
        (
            std::str::from_utf8(&cursor).unwrap().parse().unwrap(),
            elements,
        )
    }

    fn page(
        cache: &MyCache,
        base: &HScanParams,
        cursor: u64,
        replicated: bool,
    ) -> (u64, Vec<Bytes>) {
        let params = HScanParams {
            cursor,
            ..base.clone()
        };
        unpack(params.execute(cache, 0, 200, replicated))
    }

    fn scan_all(cache: &MyCache, base: &HScanParams, replicated: bool) -> Vec<Bytes> {
        let mut cursor = 0;
        let mut collected = Vec::new();
        let mut pages = 0;
        loop {
            let (next, elements) = page(cache, base, cursor, replicated);
            collected.extend(elements);
            pages += 1;
            assert!(pages < 10_000, "the cursor never reached the end");
            if next == 0 {
                return collected;
            }
            assert_ne!(next, cursor, "the cursor must advance");
            cursor = next;
        }
    }

    fn fields(
        count: std::ops::Range<u16>,
        suffix: &str,
    ) -> impl DoubleEndedIterator<Item = String> {
        let suffix = suffix.to_string();
        count.map(move |index| format!("field:{index}{suffix}"))
    }

    #[test]
    fn full_iteration_returns_every_field_and_value_once() {
        let cache = seeded(fields(0..40, ""));
        let collected = scan_all(
            &cache,
            &params(&["HSCAN", "h", "0", "COUNT", "7"]).unwrap(),
            false,
        );
        assert_eq!(collected.len(), 80);

        let mut names = Vec::new();
        for pair in collected.chunks(2) {
            let value = format!("value:{}", String::from_utf8_lossy(&pair[0]));
            assert_eq!(pair[1], Bytes::from(value));
            names.push(pair[0].clone());
        }
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 40, "a field was returned twice");
    }

    #[test]
    fn no_values_and_match_filter_the_page() {
        let cache = seeded(fields(0..20, "").chain(fields(0..5, ":keep")));
        let collected = scan_all(
            &cache,
            &params(&["HSCAN", "h", "0", "MATCH", "*keep", "NOVALUES"]).unwrap(),
            false,
        );
        let mut expected: Vec<Bytes> = fields(0..5, ":keep").map(Bytes::from).collect();
        expected.sort();
        let mut collected = collected;
        collected.sort();
        assert_eq!(collected, expected);

        // Without NOVALUES the same fields carry their values.
        let collected = scan_all(
            &cache,
            &params(&["HSCAN", "h", "0", "MATCH", "*keep"]).unwrap(),
            false,
        );
        assert_eq!(collected.len(), 10);
    }

    #[test]
    fn replicas_paginate_identically_and_share_cursors() {
        let forward = seeded(fields(0..30, ""));
        let reverse = seeded(fields(0..30, "").rev());
        let base = params(&["HSCAN", "h", "0", "COUNT", "4"]).unwrap();

        let left = scan_all(&forward, &base, true);
        let right = scan_all(&reverse, &base, true);
        assert_eq!(left, right, "replicas must return the same pages");
        assert_eq!(left.len(), 60);

        let (cursor, first) = page(&forward, &base, 0, false);
        assert_ne!(cursor, 0);
        assert_eq!(
            page(&forward, &base, cursor, false),
            page(&reverse, &base, cursor, false)
        );
        assert!(!first.is_empty());
    }

    #[test]
    fn deleted_fields_disappear_between_pages() {
        let cache = seeded(fields(0..6, ""));
        let base = params(&["HSCAN", "h", "0", "COUNT", "1", "NOVALUES"]).unwrap();
        let (mut cursor, first) = page(&cache, &base, 0, false);
        assert_eq!(first.len(), 1);
        assert_ne!(cursor, 0);

        let ValueObject::Hash(map) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"h"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a hash");
        };
        map.lock().remove(&first[0]);

        let mut collected = first.clone();
        while cursor != 0 {
            let (next, fields) = page(&cache, &base, cursor, false);
            collected.extend(fields);
            cursor = next;
        }
        let unique: std::collections::HashSet<_> = collected.iter().collect();
        assert_eq!(unique.len(), collected.len(), "a field was returned twice");
        let seen = collected.iter().filter(|field| *field == &first[0]).count();
        assert_eq!(seen, 1, "a deleted field must not come back");
        assert_eq!(collected.len(), 6);
    }

    #[test]
    fn reply_encodes_like_redis_in_both_protocols() {
        let cache = seeded(std::iter::once("field".to_string()));
        let reply = HScanParams {
            key: "h".into(),
            cursor: 0,
            count: 10,
            pattern: None,
            no_values: false,
        }
        .execute(&cache, 0, 200, false);
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto),
                b"*2\r\n$1\r\n0\r\n*2\r\n$5\r\nfield\r\n$11\r\nvalue:field\r\n"
            );
        }
    }

    #[test]
    fn a_new_iteration_sees_fields_added_later() {
        let cache = seeded(fields(0..3, ""));
        let base = params(&["HSCAN", "h", "0", "NOVALUES"]).unwrap();
        assert_eq!(scan_all(&cache, &base, false).len(), 3);

        let ValueObject::Hash(map) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"h"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a hash");
        };
        map.lock()
            .insert("field:new".into(), HashValue::Str("v".into()));

        let collected = scan_all(&cache, &base, false);
        assert_eq!(collected.len(), 4);
        assert!(collected.contains(&Bytes::from_static(b"field:new")));
    }

    #[test]
    fn missing_key_wrong_type_and_invalid_arguments() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let base = params(&["HSCAN", "h", "0"]).unwrap();
        assert_eq!(
            base.execute(&cache, 0, 200, false).encode(),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );

        cache.databases[0]
            .mocha
            .insert_persistent("s".into(), MyValue::new(ValueObject::String("v".into())));
        let wrong_type = HScanParams {
            key: "s".into(),
            ..base
        }
        .execute(&cache, 0, 200, false);
        assert!(matches!(wrong_type, Value::Error(_)));

        assert!(matches!(
            params(&["HSCAN", "h"]),
            Err(ProtocolError::WrongArgCount("hscan"))
        ));
        assert_eq!(error_text(&["HSCAN", "h", "x"]), "ERR invalid cursor");
        assert_eq!(
            error_text(&["HSCAN", "h", "0", "COUNT", "0"]),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&["HSCAN", "h", "0", "COUNT", "abc"]),
            "ERR value is not an integer or out of range"
        );
        // HSCAN has no TYPE option.
        assert_eq!(
            error_text(&["HSCAN", "h", "0", "TYPE", "hash"]),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&["HSCAN", "h", "0", "MATCH"]),
            "ERR syntax error"
        );
        // Option names compare like C strings, so a NUL terminator ends one.
        assert!(
            params(&["HSCAN", "h", "0", "NOVALUES\0"])
                .unwrap()
                .no_values
        );

        let parsed = params(&[
            "HSCAN", "h", "7", "COUNT", "3", "MATCH", "a*", "NOVALUES", "count", "5",
        ])
        .unwrap();
        assert_eq!(parsed.cursor, 7);
        assert_eq!(parsed.count, 5);
        assert_eq!(parsed.pattern, Some(Bytes::from_static(b"a*")));
        assert!(parsed.no_values);
    }
}
