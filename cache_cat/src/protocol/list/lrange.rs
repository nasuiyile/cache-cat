use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::utils::lrange;
use async_trait::async_trait;

pub struct LRangeCommand;

struct RangeArgs {
    key: Vec<u8>,
    start: i64,
    stop: i64,
}

impl LRangeCommand {
    fn parse_args(items: &[Value]) -> Result<RangeArgs, ProtocolError> {
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

        Ok(RangeArgs { key, start, stop })
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

#[async_trait]
impl Command for LRangeCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = Self::parse_args(items)?;
        let my_value = server.app.read(params.key, *db_number).await?;
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
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
