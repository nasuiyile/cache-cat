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

/// ZRANGE command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeCommand;

/// Parsed arguments for ZRANGE
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRangeParams {
    pub key: Bytes,
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

impl ReadCommand for ZRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Array(Some(vec![])), // 空集合返回空数组
            Some(v) => match v.value.data {
                ZSet(list) => {
                    let res = list.lock().zrange(self.start, self.stop);

                    if self.with_scores {
                        // WITHSCORES: RESP2 flat [m1, s1, ...] with bulk-string
                        // scores; RESP3 array of [member, double] pairs.
                        Value::MemberScores(res)
                    } else {
                        // 只有成员，返回普通数组
                        let mut vec = Vec::with_capacity(res.len());
                        for (member, _) in res {
                            vec.push(Value::BulkString(Some(member)));
                        }
                        Value::Array(Some(vec))
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl ZRangeCommand {
    /// Parse ZRANGE arguments: ZRANGE key start stop [WITHSCORES]
    fn parse_args(items: &[Value]) -> Result<ZRangeParams, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zrange"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let start = items[2].try_parse_i64()?;
        let stop = items[3].try_parse_i64()?;

        let mut with_scores = false;
        for item in &items[4..] {
            let Some(flag) = item.as_str_lossy() else {
                return Err(ProtocolError::SyntaxError);
            };
            if !flag.eq_ignore_ascii_case("WITHSCORES") {
                return Err(ProtocolError::SyntaxError);
            }
            with_scores = true;
        }

        Ok(ZRangeParams {
            key,
            start,
            stop,
            with_scores,
        })
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
    fn rejects_unknown_options() {
        assert_eq!(
            ZRangeCommand::parse_args(&args(&["ZRANGE", "key", "0", "-1", "BOGUS"])).unwrap_err(),
            ProtocolError::SyntaxError
        );
    }
}
