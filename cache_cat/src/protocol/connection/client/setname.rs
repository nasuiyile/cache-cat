use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct SetNameCommand;

#[async_trait]
impl SubCommand for SetNameCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL GET-MASTER-ADDR-BY-NAME").into());
        }
        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };
        client.name = name;
        Ok(Value::ok())
    }
}
