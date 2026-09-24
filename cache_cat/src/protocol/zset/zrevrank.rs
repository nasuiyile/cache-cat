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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRevRankCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZRevRankParams {
    pub key: Bytes,
    pub member: Bytes,
}

impl ReadCommand for ZRevRankParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // key 不存在
            None => Value::BulkString(None),

            Some(v) => match v.value.data {
                ZSet(zset) => {
                    let zset = zset.lock();

                    match zset.zrevrank(&self.member) {
                        // member 存在
                        Some(rank) => Value::Integer(rank),

                        // member 不存在
                        None => Value::BulkString(None),
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
            "ZRevRankParams {{ key: {}, member: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.member)
        )
    }
}

impl ZRevRankCommand {
    fn parse_args(items: &[Value]) -> Result<ZRevRankParams, ProtocolError> {
        // ZREVRANK key member
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("zrevrank"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let member = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("member"))?;

        Ok(ZRevRankParams { key, member })
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
