//! SSCAN command implementation
//!
//! SSCAN key cursor [MATCH pattern] [COUNT count]
//!
//! Reply: array of the next cursor and the members of this page.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
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

/// Parsed SSCAN arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
}

impl Display for SScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SSCAN {} {} COUNT {}",
            String::from_utf8_lossy(&self.key),
            self.cursor,
            self.count
        )
    }
}

impl SScanParams {
    /// Read one page of the set. A replicated read rebuilds the element order,
    /// so every replica returns the same members for the same cursor.
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
        let ValueObject::Set(set) = entry.value.data else {
            return ProtocolError::WrongType.into();
        };

        let live_len = set.lock().len();
        // A new iteration must see every member that exists when it starts.
        let rebuild = replicated || self.cursor == 0;
        let index = database
            .collection_indexes
            .get(&self.key, live_len, rebuild, || {
                set.lock().iter().cloned().collect()
            });
        let guard = set.lock();
        let matcher = self
            .pattern
            .as_ref()
            .map(|pattern| GlobMatcher::new(pattern));
        let (cursor, members) = index.page(self.cursor, self.count, |member| {
            // 索引可能包含建立之后被删除的成员
            guard.contains(member)
                && matcher
                    .as_ref()
                    .is_none_or(|matcher| matcher.matches(member.as_ref()))
        });

        scan_cursor::reply(
            cursor,
            members
                .into_iter()
                .map(|member| Value::BulkString(Some(member)))
                .collect(),
        )
    }

    /// Parse `SSCAN key cursor [MATCH pattern] [COUNT count]`.
    fn parse_args(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("sscan"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let cursor = scan_cursor::parse_cursor(&items[2])?;
        let options =
            ScanOptions::parse(cursor, items.get(3..).unwrap_or_default(), |_, _| Ok(None))?;

        Ok(Self {
            key,
            cursor: options.cursor,
            count: options.count,
            pattern: options.pattern,
        })
    }
}

/// SSCAN command handler
pub struct SScanCommand;

impl ReadRaftCommand for SScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SScan(SScanParams::parse_args(items)?))
    }
}

#[async_trait]
impl Command for SScanCommand {
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
    use parking_lot::Mutex;
    use std::collections::HashSet;
    use std::sync::Arc;

    fn request(parts: &[&str]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(part.as_bytes().to_vec().into())))
            .collect()
    }

    fn params(parts: &[&str]) -> Result<SScanParams, ProtocolError> {
        SScanParams::parse_args(&request(parts))
    }

    fn error_text(parts: &[&str]) -> String {
        match SScanCommand.read_operation(&request(parts)) {
            Ok(_) => panic!("expected an error for {parts:?}"),
            Err(error) => error.to_string(),
        }
    }

    /// Cache holding one set whose members are inserted in the given order.
    fn seeded(members: impl DoubleEndedIterator<Item = String>) -> MyCache {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let set: HashSet<Bytes> = members.map(Bytes::from).collect();
        cache.databases[0].mocha.insert_persistent(
            "s".into(),
            MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
        );
        cache
    }

    fn unpack(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut items)) = reply else {
            panic!("expected a two element SSCAN reply: {reply:?}");
        };
        let Some(Value::Array(Some(elements))) = items.pop() else {
            panic!("expected a member array: {items:?}");
        };
        let Some(Value::BulkString(Some(cursor))) = items.pop() else {
            panic!("expected a bulk string cursor: {items:?}");
        };
        let elements = elements
            .into_iter()
            .map(|element| match element {
                Value::BulkString(Some(element)) => element,
                other => panic!("expected a bulk string member: {other:?}"),
            })
            .collect();
        (
            std::str::from_utf8(&cursor).unwrap().parse().unwrap(),
            elements,
        )
    }

    fn page(
        cache: &MyCache,
        base: &SScanParams,
        cursor: u64,
        replicated: bool,
    ) -> (u64, Vec<Bytes>) {
        let params = SScanParams {
            cursor,
            ..base.clone()
        };
        unpack(params.execute(cache, 0, 200, replicated))
    }

    fn scan_all(cache: &MyCache, base: &SScanParams, replicated: bool) -> Vec<Bytes> {
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

    fn members(
        count: std::ops::Range<u16>,
        suffix: &str,
    ) -> impl DoubleEndedIterator<Item = String> {
        let suffix = suffix.to_string();
        count.map(move |index| format!("member:{index}{suffix}"))
    }

    #[test]
    fn full_iteration_returns_every_member_once() {
        let cache = seeded(members(0..40, ""));
        let collected = scan_all(
            &cache,
            &params(&["SSCAN", "s", "0", "COUNT", "7"]).unwrap(),
            false,
        );
        let unique: std::collections::HashSet<_> = collected.iter().collect();
        assert_eq!(unique.len(), 40, "a member was returned twice");
    }

    #[test]
    fn match_filters_the_page() {
        let cache = seeded(members(0..6, ""));
        let collected = scan_all(
            &cache,
            &params(&["SSCAN", "s", "0", "MATCH", "member:*"]).unwrap(),
            false,
        );
        assert_eq!(collected.len(), 6);

        let mut matched = scan_all(
            &cache,
            &params(&["SSCAN", "s", "0", "MATCH", "member:1*"]).unwrap(),
            false,
        );
        matched.sort();
        assert_eq!(matched, vec![Bytes::from_static(b"member:1")]);

        let (cursor, none) = page(
            &cache,
            &params(&["SSCAN", "s", "0", "MATCH", "nope*"]).unwrap(),
            0,
            false,
        );
        assert_eq!(cursor, 0);
        assert!(none.is_empty());
    }

    #[test]
    fn replicas_paginate_identically_and_share_cursors() {
        let forward = seeded(members(0..30, ""));
        let reverse = seeded(members(0..30, "").rev());
        let base = params(&["SSCAN", "s", "0", "COUNT", "4"]).unwrap();

        assert_eq!(
            scan_all(&forward, &base, true),
            scan_all(&reverse, &base, true),
            "replicas must return the same pages"
        );

        let (cursor, first) = page(&forward, &base, 0, false);
        assert_ne!(cursor, 0);
        assert!(!first.is_empty());
        assert_eq!(
            page(&forward, &base, cursor, false),
            page(&reverse, &base, cursor, false)
        );
    }

    #[test]
    fn deleted_members_disappear_between_pages() {
        let cache = seeded(members(0..6, ""));
        let base = params(&["SSCAN", "s", "0", "COUNT", "1"]).unwrap();
        let (mut cursor, first) = page(&cache, &base, 0, false);
        assert_eq!(first.len(), 1);
        assert_ne!(cursor, 0);

        let ValueObject::Set(set) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"s"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a set");
        };
        set.lock().remove(&first[0]);

        let mut collected = first.clone();
        while cursor != 0 {
            let (next, elements) = page(&cache, &base, cursor, false);
            collected.extend(elements);
            cursor = next;
        }
        let unique: std::collections::HashSet<_> = collected.iter().collect();
        assert_eq!(unique.len(), collected.len(), "a member was returned twice");
        let seen = collected
            .iter()
            .filter(|member| *member == &first[0])
            .count();
        assert_eq!(seen, 1, "a deleted member must not come back");
        assert_eq!(collected.len(), 6);
    }

    #[test]
    fn reply_encodes_like_redis_in_both_protocols() {
        let cache = seeded(std::iter::once("only".to_string()));
        let reply = SScanParams {
            key: "s".into(),
            cursor: 0,
            count: 10,
            pattern: None,
        }
        .execute(&cache, 0, 200, false);
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto),
                b"*2\r\n$1\r\n0\r\n*1\r\n$4\r\nonly\r\n"
            );
        }
    }

    #[test]
    fn a_new_iteration_sees_members_added_later() {
        let cache = seeded(members(0..3, ""));
        let base = params(&["SSCAN", "s", "0"]).unwrap();
        assert_eq!(scan_all(&cache, &base, false).len(), 3);

        let ValueObject::Set(set) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"s"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a set");
        };
        set.lock().insert("member:new".into());

        let collected = scan_all(&cache, &base, false);
        assert_eq!(collected.len(), 4);
        assert!(collected.contains(&Bytes::from_static(b"member:new")));
    }

    #[test]
    fn missing_key_wrong_type_and_invalid_arguments() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let base = params(&["SSCAN", "s", "0"]).unwrap();
        assert_eq!(
            base.execute(&cache, 0, 200, false).encode(),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );

        cache.databases[0]
            .mocha
            .insert_persistent("str".into(), MyValue::new(ValueObject::String("v".into())));
        let wrong_type = SScanParams {
            key: "str".into(),
            ..base
        }
        .execute(&cache, 0, 200, false);
        assert!(matches!(wrong_type, Value::Error(_)));

        assert!(matches!(
            params(&["SSCAN", "s"]),
            Err(ProtocolError::WrongArgCount("sscan"))
        ));
        assert_eq!(error_text(&["SSCAN", "s", "x"]), "ERR invalid cursor");
        assert_eq!(
            error_text(&["SSCAN", "s", "0", "COUNT", "0"]),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&["SSCAN", "s", "0", "COUNT", "abc"]),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            error_text(&["SSCAN", "s", "0", "TYPE", "set"]),
            "ERR syntax error"
        );

        let parsed = params(&["SSCAN", "s", "9", "COUNT", "3", "MATCH", "a*"]).unwrap();
        assert_eq!(parsed.cursor, 9);
        assert_eq!(parsed.count, 3);
        assert_eq!(parsed.pattern, Some(Bytes::from_static(b"a*")));
    }
}
