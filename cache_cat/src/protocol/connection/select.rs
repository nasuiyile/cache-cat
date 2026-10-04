use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;

pub struct SelectCommand;

impl SelectCommand {
    fn database(items: &[Value], database_count: usize) -> Result<u16, ProtocolError> {
        let bytes = items[1]
            .string_bytes_clone()
            .ok_or_else(|| ProtocolError::response("ERR invalid DB index"))?;
        let index = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| text.parse::<i64>().ok())
            .filter(|index| index.to_string().as_bytes() == bytes.as_ref())
            .ok_or_else(|| ProtocolError::response("ERR invalid DB index"))?;
        let num = u16::try_from(index).map_err(|_| ProtocolError::DbNotExist)?;
        if usize::from(num) >= database_count {
            return Err(ProtocolError::DbNotExist);
        }
        Ok(num)
    }
}

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
        let database = Self::database(items, server.app.state_machine.data.kvs.databases.len());
        if let Some(queue) = client.transaction_queue.as_mut() {
            // Bind subsequent operations to the selected database while keeping
            // the connection's database unchanged until EXEC succeeds.
            let reply = match database {
                Ok(db_number) => {
                    queue.db_number = db_number;
                    Value::ok()
                }
                Err(error) => error.into(),
            };
            queue.push(Operation::Redis(RedisOperation::RedisReply(reply)));
            return Ok(Value::queued());
        }
        client.db_number = database?;
        Ok(Value::ok())
    }
}
