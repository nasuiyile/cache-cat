//! ZRANGE command implementation.
//!
//! `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::sorted_set::{LexBound, ScoreBound};
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ZRangeSpec {
    Rank {
        start: i64,
        stop: i64,
    },
    Score {
        first: ScoreBound,
        second: ScoreBound,
    },
    Lex {
        first: LexBound,
        second: LexBound,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeParams {
    pub key: Bytes,
    pub spec: ZRangeSpec,
    pub reverse: bool,
    pub limit: Option<(i64, i64)>,
    pub with_scores: bool,
}

impl Display for ZRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRangeParams {{ key: {}, spec: {:?}, reverse: {}, limit: {:?}, with_scores: {} }}",
            String::from_utf8_lossy(&self.key),
            self.spec,
            self.reverse,
            self.limit,
            self.with_scores
        )
    }
}

impl ReadCommand for ZRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(v) = value else {
            return Value::Array(Some(Vec::new()));
        };
        let ZSet(zset) = v.value.data else {
            return ProtocolError::WrongType.into();
        };
        let zset = zset.lock();
        let result = match &self.spec {
            ZRangeSpec::Rank { start, stop } => zset.zrange_rank(*start, *stop, self.reverse),
            ZRangeSpec::Score { first, second } => {
                zset.zrange_score(first, second, self.reverse, self.limit)
            }
            ZRangeSpec::Lex { first, second } => {
                zset.zrange_lex(first, second, self.reverse, self.limit)
            }
        };

        if self.with_scores {
            Value::MemberScores(result)
        } else {
            Value::Array(Some(
                result
                    .into_iter()
                    .map(|(member, _)| Value::BulkString(Some(member)))
                    .collect(),
            ))
        }
    }
}

impl ZRangeCommand {
    fn parse_args(items: &[Value]) -> Result<ZRangeParams, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zrange"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let mut mode = None;
        let mut reverse = false;
        let mut limit = None;
        let mut with_scores = false;
        let mut i = 4;
        while i < items.len() {
            let option = option_string(&items[i])?;
            match option.as_str() {
                "BYSCORE" => {
                    if mode.is_some() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    mode = Some(Mode::Score);
                    i += 1;
                }
                "BYLEX" => {
                    if mode.is_some() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    mode = Some(Mode::Lex);
                    i += 1;
                }
                "REV" => {
                    if reverse {
                        return Err(ProtocolError::SyntaxError);
                    }
                    reverse = true;
                    i += 1;
                }
                "WITHSCORES" => {
                    // Redis treats repeated WITHSCORES as the same flag.
                    with_scores = true;
                    i += 1;
                }
                "LIMIT" => {
                    if i + 2 >= items.len() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    let offset = items[i + 1].try_parse_canonical_i64()?;
                    let count = items[i + 2].try_parse_canonical_i64()?;
                    limit = Some((offset, count));
                    i += 3;
                }
                _ => return Err(ProtocolError::SyntaxError),
            }
        }

        let spec = match mode {
            None => {
                if let Some((_, count)) = limit {
                    // Redis uses -1 as the internal "no limit" sentinel;
                    // consequently LIMIT <offset> -1 is accepted in rank
                    // mode and has no effect. Other LIMIT forms are rejected.
                    if count != -1 {
                        return Err(ProtocolError::response(
                            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
                        ));
                    }
                    limit = None;
                }
                ZRangeSpec::Rank {
                    start: items[2].try_parse_canonical_i64()?,
                    stop: items[3].try_parse_canonical_i64()?,
                }
            }
            Some(Mode::Score) => ZRangeSpec::Score {
                first: parse_score_bound(&items[2])?,
                second: parse_score_bound(&items[3])?,
            },
            Some(Mode::Lex) => {
                if with_scores {
                    return Err(ProtocolError::response(
                        "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
                    ));
                }
                ZRangeSpec::Lex {
                    first: parse_lex_bound(&items[2])?,
                    second: parse_lex_bound(&items[3])?,
                }
            }
        };

        Ok(ZRangeParams {
            key,
            spec,
            reverse,
            limit,
            with_scores,
        })
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Score,
    Lex,
}

fn option_string(value: &Value) -> Result<String, ProtocolError> {
    Ok(value
        .as_str_lossy()
        .ok_or(ProtocolError::SyntaxError)?
        .to_ascii_uppercase())
}

fn parse_score_bound(value: &Value) -> Result<ScoreBound, ProtocolError> {
    if let Value::Integer(number) = value {
        return Ok(ScoreBound {
            value: *number as f64,
            exclusive: false,
        });
    }
    let bytes = value
        .string_bytes_clone()
        .ok_or_else(|| ProtocolError::response("ERR min or max is not a float"))?;
    let exclusive = bytes.first() == Some(&b'(');
    let data = if exclusive { &bytes[1..] } else { &bytes[..] };
    let text = std::str::from_utf8(data)
        .map_err(|_| ProtocolError::response("ERR min or max is not a float"))?;
    let number = match text.to_ascii_lowercase().as_str() {
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        "+inf" | "inf" | "+infinity" | "infinity" => f64::INFINITY,
        _ => text
            .parse::<f64>()
            .map_err(|_| ProtocolError::response("ERR min or max is not a float"))?,
    };
    if number.is_nan() {
        return Err(ProtocolError::response("ERR min or max is not a float"));
    }
    Ok(ScoreBound {
        value: number,
        exclusive,
    })
}

fn parse_lex_bound(value: &Value) -> Result<LexBound, ProtocolError> {
    let bytes = value.string_bytes_clone().ok_or_else(|| {
        ProtocolError::response("ERR min or max is not a valid string range item")
    })?;
    match bytes.as_ref() {
        b"-" => Ok(LexBound::NegativeInfinity),
        b"+" => Ok(LexBound::PositiveInfinity),
        [b'[', rest @ ..] => Ok(LexBound::Value {
            value: Bytes::copy_from_slice(rest),
            exclusive: false,
        }),
        [b'(', rest @ ..] => Ok(LexBound::Value {
            value: Bytes::copy_from_slice(rest),
            exclusive: true,
        }),
        _ => Err(ProtocolError::response(
            "ERR min or max is not a valid string range item",
        )),
    }
}

impl ReadRaftCommand for ZRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZRange(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZRangeCommand {
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
    use crate::protocol::zset::zadd::ZAddReq;
    use crate::raft::types::core::structure::sorted_set::SortedSet;

    fn args(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    #[test]
    fn parses_all_extended_options() {
        let parsed = ZRangeCommand::parse_args(&args(&[
            "ZRANGE", "key", "2", "1", "REV", "BYSCORE", "LIMIT", "-1", "-1",
        ]))
        .unwrap();
        assert!(parsed.reverse);
        assert_eq!(parsed.limit, Some((-1, -1)));
        assert!(matches!(parsed.spec, ZRangeSpec::Score { .. }));
    }

    #[test]
    fn rejects_invalid_combinations() {
        for values in [
            vec!["ZRANGE", "key", "0", "-1", "LIMIT", "0", "1"],
            vec!["ZRANGE", "key", "0", "-1", "BYSCORE", "BYLEX"],
            vec!["ZRANGE", "key", "0", "-1", "BYSCORE", "LIMIT", "0"],
            vec!["ZRANGE", "key", "0", "-1", "BYLEX", "WITHSCORES"],
        ] {
            assert!(ZRangeCommand::parse_args(&args(&values)).is_err());
        }
    }

    #[test]
    fn parses_score_and_lex_boundaries() {
        let score =
            ZRangeCommand::parse_args(&args(&["ZRANGE", "key", "(1", "+inf", "BYSCORE"])).unwrap();
        assert!(matches!(score.spec, ZRangeSpec::Score { .. }));
        let lex = ZRangeCommand::parse_args(&args(&["ZRANGE", "key", "[a", "(z", "BYLEX", "REV"]))
            .unwrap();
        assert!(matches!(lex.spec, ZRangeSpec::Lex { .. }));
    }

    fn sorted_set(members: &[(&[u8], f64)]) -> SortedSet {
        let mut set = SortedSet::new();
        set.zadd(ZAddReq {
            key: Bytes::from_static(b"key"),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            members: members
                .iter()
                .map(|(member, score)| (Bytes::copy_from_slice(member), *score))
                .collect(),
        });
        set
    }

    #[test]
    fn executes_rank_score_and_lex_modes() {
        let set = sorted_set(&[(b"a", 1.0), (b"b", 1.0), (b"c", 2.0), (b"d", 3.0)]);
        assert_eq!(
            set.zrange_rank(0, -1, true)
                .into_iter()
                .map(|(member, _)| member)
                .collect::<Vec<_>>(),
            vec![
                Bytes::from_static(b"d"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"a")
            ]
        );
        let score = set.zrange_score(
            &ScoreBound {
                value: 3.0,
                exclusive: false,
            },
            &ScoreBound {
                value: 1.0,
                exclusive: true,
            },
            true,
            Some((0, -1)),
        );
        assert_eq!(
            score
                .into_iter()
                .map(|(member, _)| member)
                .collect::<Vec<_>>(),
            vec![Bytes::from_static(b"d"), Bytes::from_static(b"c")]
        );

        let same_score = sorted_set(&[(b"a", 1.0), (b"b", 1.0), (b"c", 1.0), (b"d", 1.0)]);
        let lex = same_score.zrange_lex(
            &LexBound::Value {
                value: Bytes::from_static(b"d"),
                exclusive: false,
            },
            &LexBound::Value {
                value: Bytes::from_static(b"b"),
                exclusive: true,
            },
            true,
            Some((0, -1)),
        );
        assert_eq!(
            lex.into_iter()
                .map(|(member, _)| member)
                .collect::<Vec<_>>(),
            vec![Bytes::from_static(b"d"), Bytes::from_static(b"c")]
        );
    }

    #[test]
    fn limit_matches_redis_signed_semantics() {
        let set = sorted_set(&[(b"a", 1.0), (b"b", 2.0)]);
        let values = set.zrange_score(
            &ScoreBound {
                value: 1.0,
                exclusive: false,
            },
            &ScoreBound {
                value: 2.0,
                exclusive: false,
            },
            false,
            Some((-1, -1)),
        );
        assert!(values.is_empty());
        let values = set.zrange_score(
            &ScoreBound {
                value: 1.0,
                exclusive: false,
            },
            &ScoreBound {
                value: 2.0,
                exclusive: false,
            },
            false,
            Some((0, -1)),
        );
        assert_eq!(values.len(), 2);
    }
}
