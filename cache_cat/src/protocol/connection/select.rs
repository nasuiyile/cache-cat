use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct SelectCommand;

#[async_trait]
impl Command for SelectCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() > 2 {
            return Err(ProtocolError::WrongArgCount("select").into());
        }
        let mut num: u16 = 0;
        if items.len() == 2 {
            match &items[1] {
                Value::Integer(s) => num = *s as u16,
                Value::SimpleString(s) => {
                    num = str::from_utf8(s)
                        .ok()
                        .and_then(|v| v.parse::<u16>().ok())
                        .ok_or(ProtocolError::SyntaxError)?;
                }
                Value::BulkString(Some(bytes)) => {
                    num = std::str::from_utf8(bytes)
                        .map_err(|_| ProtocolError::WrongArgCount("select"))?
                        .parse::<u16>()
                        .map_err(|_| ProtocolError::WrongArgCount("select"))?;
                }
                _ => return Err(CacheCatError::from(ProtocolError::SyntaxError)),
            }
        }
        let len = server.app.state_machine.data.kvs.databases.len();
        if num >= len as u16 {
            return Err(ProtocolError::DbNotExist.into());
        }
        client.db_number = num;
        Ok(Value::ok())
    }
}
