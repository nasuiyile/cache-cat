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
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("select").into());
        }
        let bytes = items[1]
            .string_bytes_clone()
            .ok_or_else(|| ProtocolError::response("ERR invalid DB index"))?;
        let index = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|index| index.to_string().as_bytes() == bytes.as_ref())
            .ok_or_else(|| ProtocolError::response("ERR invalid DB index"))?;
        let num = u16::try_from(index).map_err(|_| ProtocolError::DbNotExist)?;
        let len = server.app.state_machine.data.kvs.databases.len();
        if usize::from(num) >= len {
            return Err(ProtocolError::DbNotExist.into());
        }
        client.db_number = num;
        Ok(Value::ok())
    }
}
