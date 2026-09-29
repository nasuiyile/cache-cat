use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::protocol::zset::zrange::parse_score_bound;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::sorted_set::ScoreBound;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// ZRANGEBYSCORE command handler
///
/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
/// Returns all the elements in the sorted set at key with a score between min and max.
/// The elements are considered to be ordered from low to high scores.
///
/// Options:
/// - WITHSCORES: Return scores together with elements
/// - LIMIT offset count: Skip offset elements and return only count elements
///
/// Return value:
/// - Array of members (or member, score, member, score, ... with WITHSCORES)
/// - Empty array if key does not exist or no elements in range
/// - WRONGTYPE error if key exists but is not a sorted set
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeByScoreCommand;

/// Parsed arguments for ZRANGEBYSCORE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeByScoreParams {
    pub key: Bytes,
    pub min: ScoreBound,
    pub max: ScoreBound,
    pub with_scores: bool,
    pub limit: Option<(i64, i64)>, // (offset, count)
}

impl ReadCommand for ZRangeByScoreParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.value.data {
                ZSet(list) => {
                    let zset = list.lock();
                    let res = zset.zrange_score(&self.min, &self.max, false, self.limit);

                    if self.with_scores {
                        // WITHSCORES: RESP2 flat [m1, s1, ...] with bulk-string
                        // scores; RESP3 array of [member, double] pairs.
                        Value::MemberScores(res)
                    } else {
                        let mut vec = Vec::with_capacity(res.len());
                        for (member, _) in res {
                            vec.push(Value::BulkString(Some(member)));
                        }
                        Value::Array(Some(vec))
                    }
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}
impl Display for ZRangeByScoreParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRangeByScoreParams {{ key: {}, min: {:?}, max: {:?}, with_scores: {}, limit: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.min,
            self.max,
            self.with_scores,
            self.limit
        )
    }
}

impl ZRangeByScoreCommand {
    /// Parse ZRANGEBYSCORE arguments: ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    fn parse_args(items: &[Value]) -> Result<ZRangeByScoreParams, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zrangebyscore"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // Parse min score
        let min = parse_score_bound(&items[2])?;

        // Parse max score
        let max = parse_score_bound(&items[3])?;

        // Parse optional arguments
        let mut with_scores = false;
        let mut limit = None;

        if items.len() > 4 {
            let mut i = 4;
            while i < items.len() {
                let Some(flag) = items[i].as_str_lossy() else {
                    return Err(ProtocolError::SyntaxError);
                };

                match flag.to_uppercase().as_str() {
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        // LIMIT requires offset and count
                        if i + 2 >= items.len() {
                            return Err(ProtocolError::SyntaxError);
                        }

                        let offset = items[i + 1].try_parse_canonical_i64()?;
                        let count = items[i + 2].try_parse_canonical_i64()?;
                        limit = Some((offset, count));
                        i += 3;
                    }
                    _ => {
                        return Err(ProtocolError::SyntaxError);
                    }
                }
            }
        }

        Ok(ZRangeByScoreParams {
            key,
            min,
            max,
            with_scores,
            limit,
        })
    }
}

impl ReadRaftCommand for ZRangeByScoreCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZRangeByScore(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZRangeByScoreCommand {
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
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn args(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    fn execute(values: &[&str]) -> Value {
        let mut zset = SortedSet::new();
        zset.zadd(ZAddReq {
            key: Bytes::from_static(b"key"),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            members: vec![
                (Bytes::from_static(b"a"), 1.0),
                (Bytes::from_static(b"b"), 2.0),
                (Bytes::from_static(b"c"), 3.0),
            ],
        });
        ZRangeByScoreCommand::parse_args(&args(values))
            .unwrap()
            .execute(Some(EntrySnapshot {
                value: MyValue::new(ZSet(Arc::new(Mutex::new(zset)))),
                expire_at: None,
            }))
    }

    #[test]
    fn excludes_open_score_boundaries_and_preserves_score_replies() {
        let reply = execute(&["ZRANGEBYSCORE", "key", "(1", "(3", "WITHSCORES"]);
        assert_eq!(reply.encode_proto(2), b"*2\r\n$1\r\nb\r\n$1\r\n2\r\n");
        assert_eq!(reply.encode_proto(3), b"*1\r\n*2\r\n$1\r\nb\r\n,2\r\n");
        assert_eq!(
            execute(&["ZRANGEBYSCORE", "key", "(2", "2"]).encode(),
            b"*0\r\n"
        );
    }

    #[test]
    fn signed_limits_match_redis_semantics() {
        for count in ["-1", "-2", "-9223372036854775808"] {
            assert_eq!(
                execute(&["ZRANGEBYSCORE", "key", "-inf", "+inf", "LIMIT", "1", count]).encode(),
                b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
            );
        }
        for (offset, count) in [("-1", "2"), ("0", "0")] {
            assert_eq!(
                execute(&[
                    "ZRANGEBYSCORE",
                    "key",
                    "-inf",
                    "+inf",
                    "LIMIT",
                    offset,
                    count,
                ])
                .encode(),
                b"*0\r\n"
            );
        }
        for invalid in ["+1", "01", "-0", "9223372036854775808"] {
            for (offset, count) in [(invalid, "1"), ("0", invalid)] {
                assert_eq!(
                    ZRangeByScoreCommand::parse_args(&args(&[
                        "ZRANGEBYSCORE",
                        "key",
                        "-inf",
                        "+inf",
                        "LIMIT",
                        offset,
                        count,
                    ]))
                    .unwrap_err()
                    .to_string(),
                    "ERR value is not an integer or out of range"
                );
            }
        }
    }

    #[test]
    fn rejects_unknown_options() {
        assert_eq!(
            ZRangeByScoreCommand::parse_args(&args(&[
                "ZRANGEBYSCORE",
                "key",
                "-inf",
                "+inf",
                "BOGUS",
            ]))
            .unwrap_err(),
            ProtocolError::SyntaxError
        );
    }

    #[test]
    fn rejects_nan_scores() {
        for boundary in ["NaN", "nan", "-nan", "(NaN", "(", "invalid"] {
            for bounds in [[boundary, "+inf"], ["-inf", boundary]] {
                assert_eq!(
                    ZRangeByScoreCommand::parse_args(&args(&[
                        "ZRANGEBYSCORE",
                        "key",
                        bounds[0],
                        bounds[1],
                    ]))
                    .unwrap_err()
                    .to_string(),
                    "ERR min or max is not a float"
                );
            }
        }
    }
}
