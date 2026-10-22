//! ZSCAN command implementation
//!
//! ZSCAN key cursor [MATCH pattern] [COUNT count]
//!
//! Reply: array of the next cursor and the members of this page, each followed
//! by its score as a bulk string in both RESP2 and RESP3, which is how Redis
//! answers this command (unlike ZRANGE ... WITHSCORES).

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::scan_cursor::{self, ScanOptions};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::{Value, format_double};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed ZSCAN arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
}

impl Display for ZScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZSCAN {} {} COUNT {}",
            String::from_utf8_lossy(&self.key),
            self.cursor,
            self.count
        )
    }
}

impl ZScanParams {
    /// Read one page of the sorted set. A replicated read rebuilds the element
    /// order, so every replica returns the same members for the same cursor.
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
        let ValueObject::ZSet(zset) = entry.value.data else {
            return ProtocolError::WrongType.into();
        };

        let live_len = zset.lock().len();
        // A new iteration must see every member that exists when it starts.
        let rebuild = replicated || self.cursor == 0;
        let index = database
            .collection_indexes
            .get(&self.key, live_len, rebuild, || {
                zset.lock()
                    .iter_scores()
                    .map(|(member, _)| member.clone())
                    .collect()
            });
        let guard = zset.lock();
        let matcher = self
            .pattern
            .as_ref()
            .map(|pattern| GlobMatcher::new(pattern));
        let (cursor, members) = index.page(self.cursor, self.count, |member| {
            // 索引可能包含建立之后被删除的成员
            guard.zscore(member).is_some()
                && matcher
                    .as_ref()
                    .is_none_or(|matcher| matcher.matches(member.as_ref()))
        });

        let mut elements = Vec::with_capacity(members.len() * 2);
        for member in members {
            let Some(score) = guard.zscore(&member) else {
                continue;
            };
            elements.push(Value::BulkString(Some(member)));
            elements.push(Value::BulkString(Some(format_double(score).into())));
        }
        scan_cursor::reply(cursor, elements)
    }

    /// Parse `ZSCAN key cursor [MATCH pattern] [COUNT count]`.
    fn parse_args(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("zscan"));
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

/// ZSCAN command handler
pub struct ZScanCommand;

impl ReadRaftCommand for ZScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZScan(ZScanParams::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZScanCommand {
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
    use crate::protocol::zset::zadd::ZAddReq;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn request(parts: &[&str]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(part.as_bytes().to_vec().into())))
            .collect()
    }

    fn params(parts: &[&str]) -> Result<ZScanParams, ProtocolError> {
        ZScanParams::parse_args(&request(parts))
    }

    fn error_text(parts: &[&str]) -> String {
        match ZScanCommand.read_operation(&request(parts)) {
            Ok(_) => panic!("expected an error for {parts:?}"),
            Err(error) => error.to_string(),
        }
    }

    /// Cache holding one sorted set whose members are added in the given order.
    fn seeded(members: impl DoubleEndedIterator<Item = (String, f64)>) -> MyCache {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let mut zset = SortedSet::new();
        zset.zadd(ZAddReq {
            key: "z".into(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            members: members
                .map(|(member, score)| (Bytes::from(member), score))
                .collect(),
        });
        cache.databases[0].mocha.insert_persistent(
            "z".into(),
            MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(zset)))),
        );
        cache
    }

    fn unpack(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut items)) = reply else {
            panic!("expected a two element ZSCAN reply: {reply:?}");
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
        base: &ZScanParams,
        cursor: u64,
        replicated: bool,
    ) -> (u64, Vec<Bytes>) {
        let params = ZScanParams {
            cursor,
            ..base.clone()
        };
        unpack(params.execute(cache, 0, 200, replicated))
    }

    fn scan_all(cache: &MyCache, base: &ZScanParams, replicated: bool) -> Vec<Bytes> {
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
    ) -> impl DoubleEndedIterator<Item = (String, f64)> {
        let suffix = suffix.to_string();
        count.map(move |index| (format!("member:{index}{suffix}"), f64::from(index) + 0.5))
    }

    #[test]
    fn full_iteration_returns_every_member_and_score_once() {
        let cache = seeded(members(0..40, ""));
        let collected = scan_all(
            &cache,
            &params(&["ZSCAN", "z", "0", "COUNT", "7"]).unwrap(),
            false,
        );
        assert_eq!(collected.len(), 80);

        let mut names = Vec::new();
        for pair in collected.chunks(2) {
            let name = String::from_utf8_lossy(&pair[0]).to_string();
            let index: f64 = name.trim_start_matches("member:").parse().unwrap();
            assert_eq!(pair[1], Bytes::from(format_double(index + 0.5)));
            names.push(pair[0].clone());
        }
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 40, "a member was returned twice");
    }

    #[test]
    fn match_filters_members_and_scores_stay_bulk_strings() {
        let cache = seeded(members(0..6, ""));
        let matched = scan_all(
            &cache,
            &params(&["ZSCAN", "z", "0", "MATCH", "member:4"]).unwrap(),
            false,
        );
        assert_eq!(
            matched,
            vec![Bytes::from_static(b"member:4"), Bytes::from_static(b"4.5")]
        );
    }

    #[test]
    fn replicas_paginate_identically_and_share_cursors() {
        let forward = seeded(members(0..30, ""));
        let reverse = seeded(members(0..30, "").rev());
        let base = params(&["ZSCAN", "z", "0", "COUNT", "4"]).unwrap();

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
    fn removed_members_and_changed_scores_follow_the_live_set() {
        let cache = seeded(members(0..6, ""));
        let base = params(&["ZSCAN", "z", "0", "COUNT", "1"]).unwrap();
        let (mut cursor, first) = page(&cache, &base, 0, false);
        assert_eq!(first.len(), 2);
        assert_ne!(cursor, 0);

        let ValueObject::ZSet(zset) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"z"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a sorted set");
        };
        zset.lock().zrem(&[first[0].clone()]);
        let kept = Bytes::from_static(b"member:0");
        if kept != first[0] {
            zset.lock().zincrby(kept.clone(), 100.0);
        }

        let mut collected = first.clone();
        while cursor != 0 {
            let (next, elements) = page(&cache, &base, cursor, false);
            collected.extend(elements);
            cursor = next;
        }
        let seen = collected
            .iter()
            .step_by(2)
            .filter(|member| *member == &first[0])
            .count();
        assert_eq!(seen, 1, "a removed member came back");
        let names: Vec<_> = collected.iter().step_by(2).cloned().collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "a member was returned twice");
        assert_eq!(names.len(), 6);
        if let Some(position) = names.iter().position(|name| name == &kept) {
            assert_eq!(
                collected[position * 2 + 1],
                Bytes::from(format_double(100.5)),
                "scores must be read from the live sorted set"
            );
        }
    }

    #[test]
    fn reply_encodes_like_redis_in_both_protocols() {
        let cache = seeded(std::iter::once(("only".to_string(), 2.5)));
        let reply = ZScanParams {
            key: "z".into(),
            cursor: 0,
            count: 10,
            pattern: None,
        }
        .execute(&cache, 0, 200, false);
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto),
                b"*2\r\n$1\r\n0\r\n*2\r\n$4\r\nonly\r\n$3\r\n2.5\r\n"
            );
        }
    }

    #[test]
    fn a_new_iteration_sees_members_added_later() {
        let cache = seeded(members(0..3, ""));
        let base = params(&["ZSCAN", "z", "0"]).unwrap();
        assert_eq!(scan_all(&cache, &base, false).len(), 6);

        let ValueObject::ZSet(zset) = cache.databases[0]
            .mocha
            .get_with_read_clock(&Bytes::from_static(b"z"), None)
            .unwrap()
            .value
            .data
        else {
            panic!("the test key must stay a sorted set");
        };
        zset.lock().zincrby("member:new".into(), 1.0);

        let collected = scan_all(&cache, &base, false);
        assert_eq!(collected.len(), 8);
        assert!(collected.contains(&Bytes::from_static(b"member:new")));
    }

    #[test]
    fn missing_key_wrong_type_and_invalid_arguments() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let base = params(&["ZSCAN", "z", "0"]).unwrap();
        assert_eq!(
            base.execute(&cache, 0, 200, false).encode(),
            b"*2\r\n$1\r\n0\r\n*0\r\n"
        );

        cache.databases[0]
            .mocha
            .insert_persistent("str".into(), MyValue::new(ValueObject::String("v".into())));
        let wrong_type = ZScanParams {
            key: "str".into(),
            ..base
        }
        .execute(&cache, 0, 200, false);
        assert!(matches!(wrong_type, Value::Error(_)));

        assert!(matches!(
            params(&["ZSCAN", "z"]),
            Err(ProtocolError::WrongArgCount("zscan"))
        ));
        assert_eq!(error_text(&["ZSCAN", "z", "x"]), "ERR invalid cursor");
        assert_eq!(
            error_text(&["ZSCAN", "z", "0", "COUNT", "0"]),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&["ZSCAN", "z", "0", "COUNT", "abc"]),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            error_text(&["ZSCAN", "z", "0", "TYPE", "zset"]),
            "ERR syntax error"
        );

        let parsed = params(&["ZSCAN", "z", "9", "COUNT", "3", "MATCH", "a*"]).unwrap();
        assert_eq!(parsed.cursor, 9);
        assert_eq!(parsed.count, 3);
        assert_eq!(parsed.pattern, Some(Bytes::from_static(b"a*")));
    }
}
