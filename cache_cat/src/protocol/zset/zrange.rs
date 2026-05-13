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

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// ZRANGE command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeCommand;

/// Parsed arguments for ZRANGE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeParams {
    pub key: Vec<u8>,
    pub start: i64,
    pub stop: i64,
    pub with_scores: bool,
}

impl Display for ZRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRangeParams {{ key: {}, start: {}, stop: {}, with_scores: {} }}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.stop,
            self.with_scores
        )
    }
}

impl ZRangeCommand {
    /// Parse ZRANGE arguments: ZRANGE key start stop [WITHSCORES]
    fn parse_args(items: &[Value]) -> Result<ZRangeParams, ProtocolError> {
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

        Ok(ZRangeParams {
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
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = Self::parse_args(items)?;
        let my_value = server.app.read(params.key, client.db_number).await?;
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
