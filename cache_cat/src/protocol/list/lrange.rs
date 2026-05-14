use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::utils::lrange;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;

pub struct LRangeCommand;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LRangeParams {
    pub key: Vec<u8>,
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

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let start = parse_i64(&items[2])?;
        let stop = parse_i64(&items[3])?;

        Ok(LRangeParams { key, start, stop })
    }
}

fn parse_i64(value: &Value) -> Result<i64, ProtocolError> {
    match value {
        Value::BulkString(Some(data)) => {
            let s = String::from_utf8_lossy(data);
            s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger)
        }
        Value::SimpleString(s) => s.parse::<i64>().map_err(|_| ProtocolError::NotAnInteger),
        Value::Integer(n) => Ok(*n),
        _ => Err(ProtocolError::NotAnInteger),
    }
}

impl RaftCommand for LRangeCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::LRange(params)))
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
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        let params = Self::parse_args(items)?;
        let my_value = server.app.read(params.key, client.db_number).await?;
        match my_value {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::List(list) => {
                    let vec = lrange(&list.lock(), params.start, params.stop);
                    let mut array = Vec::new();
                    for x in vec {
                        let value = Value::BulkString(Some(x.as_ref().clone()));
                        array.push(value);
                    }
                    Ok(Value::Array(Some(array)))
                }
                _ => Err(ProtocolError::WrongType.into()),
            },
        }
    }
}
