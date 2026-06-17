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

        client.name = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("master name"))?
            .to_string();

        Ok(Value::ok())
    }
}
