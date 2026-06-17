use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct SetInfoCommand;

#[async_trait]
impl SubCommand for SetInfoCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("CLIENT SETINFO").into());
        }

        let field = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("field"))?
            .to_uppercase();

        let value = items[3]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("value"))?
            .to_uppercase();

        match field.as_str() {
            "LIB-NAME" => {
                client.lib_name = value;
            }
            "LIB-VER" => {
                client.lib_ver = value;
            }
            _ => {
                return Err(ProtocolError::InvalidArgument("SETINFO sub-option").into());
            }
        }

        Ok(Value::ok())
    }
}
