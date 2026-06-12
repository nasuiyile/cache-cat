use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
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
    pub key: Vec<u8>,
    pub min: f64,
    pub max: f64,
    pub with_scores: bool,
    pub limit: Option<(usize, usize)>, // (offset, count)
}

impl Display for ZRangeByScoreParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRangeByScoreParams {{ key: {}, min: {}, max: {}, with_scores: {}, limit: {:?} }}",
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
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        // Parse min score
        let min = Self::parse_score(&items[2])?;

        // Parse max score
        let max = Self::parse_score(&items[3])?;

        // Parse optional arguments
        let mut with_scores = false;
        let mut limit: Option<(usize, usize)> = None;

        if items.len() > 4 {
            let mut i = 4;
            while i < items.len() {
                let flag = match &items[i] {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                    Value::SimpleString(s) => s.to_uppercase(),
                    _ => {
                        i += 1;
                        continue;
                    }
                };

                match flag.as_str() {
                    "WITHSCORES" => {
                        with_scores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        // LIMIT requires offset and count
                        if i + 2 >= items.len() {
                            return Err(ProtocolError::SyntaxError);
                        }

                        let offset = Self::parse_usize(&items[i + 1])?;
                        let count = Self::parse_usize(&items[i + 2])?;
                        limit = Some((offset, count));
                        i += 3;
                    }
                    _ => {
                        // Unknown flag, skip
                        i += 1;
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

    /// Parse a score value, supporting -inf and +inf
    fn parse_score(value: &Value) -> Result<f64, ProtocolError> {
        match value {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                Self::parse_score_string(&s)
            }
            Value::SimpleString(s) => Self::parse_score_string(s),
            Value::Integer(n) => Ok(*n as f64),
            _ => Err(ProtocolError::InvalidArgument("score")),
        }
    }

    fn parse_score_string(s: &str) -> Result<f64, ProtocolError> {
        let s_upper = s.to_uppercase();
        match s_upper.as_str() {
            "-INF" | "-INFINITY" => Ok(f64::NEG_INFINITY),
            "+INF" | "+INFINITY" => Ok(f64::INFINITY),
            _ => s
                .parse::<f64>()
                .map_err(|_| ProtocolError::InvalidArgument("score")),
        }
    }

    /// Parse usize from a Value
    fn parse_usize(value: &Value) -> Result<usize, ProtocolError> {
        match value {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<usize>().map_err(|_| ProtocolError::NotAnInteger)
            }
            Value::SimpleString(s) => s.parse::<usize>().map_err(|_| ProtocolError::NotAnInteger),
            Value::Integer(n) => {
                if *n < 0 {
                    return Err(ProtocolError::InvalidArgument("limit"));
                }
                Ok(*n as usize)
            }
            _ => Err(ProtocolError::NotAnInteger),
        }
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
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
