use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::mocha::EntrySnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitCountParams {
    pub key: Bytes,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

impl Display for BitCountParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BitCountParams {{ key: {:?}, start: {:?}, end: {:?} }}",
            self.key, self.start, self.end
        )
    }
}

impl ReadCommand for BitCountParams {
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

        let len = bytes.len() as i64;
        if len == 0 {
            return Value::Integer(0);
        }

        // If no range is specified, count all bits
        if self.start.is_none() && self.end.is_none() {
            let count: i64 = bytes.iter().map(|&byte| byte.count_ones() as i64).sum();
            return Value::Integer(count);
        }

        // Convert negative indices to positive
        let start = self.start.unwrap_or(0);
        let end = self.end.unwrap_or(-1);

        let start = if start < 0 { start + len } else { start };
        let end = if end < 0 { end + len } else { end };

        // Clamp indices to valid range
        let start = start.max(0).min(len - 1) as usize;
        let end = end.max(0).min(len - 1) as usize;

        // If start > end after conversion, return 0
        if start > end {
            return Value::Integer(0);
        }

        // Count bits in the specified range
        let count: i64 = bytes[start..=end]
            .iter()
            .map(|&byte| byte.count_ones() as i64)
            .sum();

        Value::Integer(count)
    }
}

pub struct BitCountCommand;

impl BitCountCommand {
    fn parse_args(items: &[Value]) -> Result<BitCountParams, ProtocolError> {
        if items.len() < 2 || items.len() > 4 {
            return Err(ProtocolError::WrongArgCount("bitcount"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("bitcount"))?;

        let mut start = None;
        let mut end = None;

        if items.len() >= 3 {
            start = Some(items[2].parse_i64().ok_or(ProtocolError::Custom(
                "ERR value is not an integer or out of range",
            ))?);
        }

        if items.len() == 4 {
            end = Some(items[3].parse_i64().ok_or(ProtocolError::Custom(
                "ERR value is not an integer or out of range",
            ))?);
        }

        Ok(BitCountParams { key, start, end })
    }
}

impl ReadRaftCommand for BitCountCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BitCount(BitCountCommand::parse_args(items)?))
    }
}

#[async_trait]
impl Command for BitCountCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("BITCOUNT")));
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}