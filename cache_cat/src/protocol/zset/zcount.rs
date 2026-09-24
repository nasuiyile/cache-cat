//! ZCOUNT command implementation
//!
//! ZCOUNT key min max
//! Returns the number of members in the sorted set with scores within the given range.
//!
//! Return value:
//! - Integer reply: number of elements in the specified score range
//! - 0 if key does not exist
//! - WRONGTYPE if key exists but is not a sorted set

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::read_operation::ReadOperation;

use async_trait::async_trait;

use bytes::Bytes;

use serde::{Deserialize, Serialize};

use std::fmt::Display;

/// Parsed score boundary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreRange {
    pub value: f64,

    /// true means exclusive:
    /// (1 means score > 1
    pub exclusive: bool,
}

/// Parse Redis score syntax:
///
/// 1
/// (1
/// -inf
/// +inf
/// inf
///
fn parse_score_range(value: &Bytes) -> Result<ScoreRange, ProtocolError> {
    let s = String::from_utf8_lossy(value);

    if s.starts_with('(') {
        let score = s[1..]
            .parse::<f64>()
            .map_err(|_| ProtocolError::response("ERR min or max is not a float"))?;

        if score.is_nan() {
            return Err(ProtocolError::response("ERR min or max is not a float"));
        }

        return Ok(ScoreRange {
            value: score,
            exclusive: true,
        });
    }

    let score = match s.as_ref() {
        "-inf" => f64::NEG_INFINITY,

        "+inf" | "inf" => f64::INFINITY,

        _ => s
            .parse::<f64>()
            .map_err(|_| ProtocolError::response("ERR min or max is not a float"))?,
    };

    if score.is_nan() {
        return Err(ProtocolError::response("ERR min or max is not a float"));
    }

    Ok(ScoreRange {
        value: score,
        exclusive: false,
    })
}

/// ZCOUNT command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZCountCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZCountParams {
    pub key: Bytes,

    pub min: f64,

    pub max: f64,

    pub min_exclusive: bool,

    pub max_exclusive: bool,
}

impl ReadCommand for ZCountParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // key 不存在
            None => Value::Integer(0),

            Some(v) => {
                match v.value.data {
                    ZSet(zset) => {
                        let count = zset.lock().zcount(
                            self.min,
                            self.max,
                            self.min_exclusive,
                            self.max_exclusive,
                        );

                        Value::Integer(count)
                    }

                    // 类型错误
                    _ => ProtocolError::WrongType.into(),
                }
            }
        }
    }
}

impl Display for ZCountParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZCountParams {{ key: {}, min: {}, max: {}, min_exclusive: {}, max_exclusive: {} }}",
            String::from_utf8_lossy(&self.key),
            self.min,
            self.max,
            self.min_exclusive,
            self.max_exclusive
        )
    }
}

impl ZCountCommand {
    fn parse_args(items: &[Value]) -> Result<ZCountParams, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("zcount"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let min_bytes = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("min"))?;

        let max_bytes = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("max"))?;

        let min = parse_score_range(&min_bytes)?;

        let max = parse_score_range(&max_bytes)?;

        Ok(ZCountParams {
            key,

            min: min.value,

            max: max.value,

            min_exclusive: min.exclusive,

            max_exclusive: max.exclusive,
        })
    }
}

impl ReadRaftCommand for ZCountCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZCount(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZCountCommand {
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

    fn args(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    #[test]
    fn rejects_nan_boundaries() {
        for boundary in ["NaN", "nan", "-nan", "(NaN", "invalid"] {
            for bounds in [[boundary, "+inf"], ["-inf", boundary]] {
                assert_eq!(
                    ZCountCommand::parse_args(&args(&["ZCOUNT", "key", bounds[0], bounds[1]]))
                        .unwrap_err()
                        .to_string(),
                    "ERR min or max is not a float"
                );
            }
        }
    }
}
