//! ZRANGE command implementation
//!
//! ZRANGE key start stop [WITHSCORES]
//! Returns the specified range of elements in the sorted set stored at key.
//! Elements are ordered from the lowest to the highest score.
//!
//! Start and stop are 0-based indices. Negative indices count from the end.
//! WITHSCORES returns member-score pairs.
//!
//! Return value:
//! - Array of members (or member, score, member, score, ... with WITHSCORES)
//! - Empty array if key does not exist
//! - WRONGTYPE error if key exists but is not a sorted set

use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

/// ZRANGE command handler
pub struct ZRangeCommand;

/// Parsed arguments for ZRANGE
struct ZRangeArgs {
    key: Vec<u8>,
    start: i64,
    stop: i64,
    with_scores: bool,
}

impl ZRangeCommand {
    /// Parse ZRANGE arguments: ZRANGE key start stop [WITHSCORES]
    fn parse_args(items: &[Value]) -> Result<ZRangeArgs, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zrange"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let start = match &items[2] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
            }
            Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
            Value::Integer(n) => *n,
            _ => return Err(ProtocolError::NotAnInteger),
        };

        let stop = match &items[3] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?
            }
            Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)?,
            Value::Integer(n) => *n,
            _ => return Err(ProtocolError::NotAnInteger),
        };

        // Check for WITHSCORES flag
        let mut with_scores = false;
        if items.len() > 4 {
            for item in &items[4..] {
                let flag = match item {
                    Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
                    Value::SimpleString(s) => s.clone(),
                    _ => continue,
                };
                if flag.to_uppercase() == "WITHSCORES" {
                    with_scores = true;
                }
            }
        }

        Ok(ZRangeArgs {
            key,
            start,
            stop,
            with_scores,
        })
    }
}

#[async_trait]
impl Command for ZRangeCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = Self::parse_args(items)?;
        let my_value = server.app.read(params.key, *db_number).await?;
        match my_value {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::ZSet(list) => {
                    let res = list
                        .lock()
                        .zrange(params.start, params.stop, params.with_scores);
                    let mut vec = Vec::new();
                    for v in res {
                        vec.push(Value::BulkString(Some(v)))
                    }
                    Ok(Value::Array(Some(vec)))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
