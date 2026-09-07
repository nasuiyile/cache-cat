use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
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
impl ReadCommand for GetBitParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let bytes: Vec<u8> = match value {
            None => return Value::Integer(0),
            Some(value) => match value.value.data {
                ValueObject::String(s) => s.to_vec(),
                ValueObject::Int(i) => i.to_string().into_bytes(),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        let offset = self.offset; // u64
        let byte_index = (offset / 8) as usize;
        let bit_offset = (offset % 8) as usize;
        let bit = if byte_index >= bytes.len() {
            0
        } else {
            let byte = bytes[byte_index];
            ((byte >> (7 - bit_offset)) & 1) as i64
        };
        Value::Integer(bit)
    }
}
pub struct GetBitCommand;

impl GetBitCommand {
    fn parse_args(items: &[Value]) -> Result<GetBitParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("getbit"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("getbit"))?;

        let offset = items[2].parse_u64().ok_or(ProtocolError::response(
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
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
