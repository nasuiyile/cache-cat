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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBitParams {
    pub key: Bytes,
    pub offset: u64,
}

impl Display for GetBitParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetBitParams {{ key: {:?}, offset: {} }}",
            self.key, self.offset
        )
    }
}

pub struct GetBitCommand;

impl GetBitCommand {
    fn parse_args(items: &[Value]) -> Result<GetBitParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("getbit"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("rename"))?
            .clone();

        let offset = items[2].parse_u64().ok_or(ProtocolError::Custom(
            "ERR bit offset is not an integer or out of range",
        ))?;

        Ok(GetBitParams { key, offset })
    }
}

impl ReadRaftCommand for GetBitCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::GetBit(GetBitCommand::parse_args(items)?))
    }
}

#[async_trait]
impl Command for GetBitCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("GETBIT"));
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
