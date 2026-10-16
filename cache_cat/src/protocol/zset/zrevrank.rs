use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRevRankCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRevRankParams {
    pub key: Bytes,
    pub member: Bytes,
    /// Redis 7.2+: return the member score together with its reverse rank.
    pub with_score: bool,
}

impl ReadCommand for ZRevRankParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let missing = if self.with_score {
            Value::Array(None)
        } else {
            Value::BulkString(None)
        };
        match value {
            // key 不存在
            None => missing,

            Some(v) => match v.value.data {
                ZSet(zset) => {
                    let zset = zset.lock();

                    match zset.zrevrank(&self.member) {
                        // member 存在
                        Some(rank) if self.with_score => match zset.zscore(&self.member) {
                            Some(score) => {
                                Value::Array(Some(vec![Value::Integer(rank), Value::Double(score)]))
                            }
                            None => missing,
                        },
                        Some(rank) => Value::Integer(rank),

                        // member 不存在
                        None => missing,
                    }
                }

                // key 存在但不是 sorted set
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl Display for ZRevRankParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZRevRankParams {{ key: {}, member: {}, with_score: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.member),
            self.with_score
        )
    }
}

impl ZRevRankCommand {
    fn parse_args(items: &[Value]) -> Result<ZRevRankParams, ProtocolError> {
        // ZREVRANK key member
        if !(3..=4).contains(&items.len()) {
            return Err(ProtocolError::WrongArgCount("zrevrank"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let member = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("member"))?;

        let with_score = if items.len() == 4 {
            let option = items[3]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"WITHSCORE") {
                true
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        } else {
            false
        };

        Ok(ZRevRankParams {
            key,
            member,
            with_score,
        })
    }
}

impl ReadRaftCommand for ZRevRankCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZRevRank(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZRevRankCommand {
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
