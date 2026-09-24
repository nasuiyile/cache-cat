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
pub struct ZScoreCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZScoreParams {
    pub key: Bytes,
    pub member: Bytes,
}

impl ReadCommand for ZScoreParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // key 不存在
            None => Value::Null,

            Some(v) => match v.value.data {
                ZSet(zset) => {
                    let zset = zset.lock();

                    match zset.zscore(&self.member) {
                        // Double reply: RESP2 bulk string, RESP3 double.
                        Some(score) => Value::Double(score),

                        None => Value::Null,
                    }
                }

                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl Display for ZScoreParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZScoreParams {{ key: {}, member: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.member)
        )
    }
}

impl ZScoreCommand {
    fn parse_args(items: &[Value]) -> Result<ZScoreParams, ProtocolError> {
        // ZSCORE key member
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("zscore"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let member = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("member"))?;

        Ok(ZScoreParams { key, member })
    }
}

impl ReadRaftCommand for ZScoreCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZScore(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZScoreCommand {
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
