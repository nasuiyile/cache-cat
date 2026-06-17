use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub struct LRangeCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LRangeParams {
    pub key: Bytes,
    pub start: i64,
    pub stop: i64,
}

impl Display for LRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LRangeParams {{ key: {}, start: {}, stop: {} }}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.stop
        )
    }
}

impl LRangeCommand {
    fn parse_args(items: &[Value]) -> Result<LRangeParams, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("lrange"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let start = items[2].try_parse_i64()?;
        let stop = items[3].try_parse_i64()?;

        Ok(LRangeParams { key, start, stop })
    }
}

impl ReadRaftCommand for LRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::LRange(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for LRangeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
