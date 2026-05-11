use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use async_trait::async_trait;

pub struct HGetCommand;

#[async_trait]
impl Command for HGetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse HGET key field
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hget").into());
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key").into()),
        };

        let field = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("field").into()),
        };
        let my_value = server.app.read(key, client.db_number).await?;

        match my_value {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&field);
                    match option {
                        None => Ok(Value::BulkString(None)),
                        Some(value) => match value {
                            HashValue::Str(str) => {
                                Ok(Value::BulkString(Some(str.as_ref().clone())))
                            }
                            HashValue::Int(int) => {
                                Ok(Value::BulkString(Some(int.to_string().as_bytes().to_vec())))
                            }
                        },
                    }
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
