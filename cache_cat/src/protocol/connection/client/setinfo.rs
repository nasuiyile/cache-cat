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
            return Err(
                ProtocolError::WrongArgCount("CLIENT SETINFO").into()
            );
        }

        let field = match &items[2] {
            Value::BulkString(Some(data)) => {
                String::from_utf8_lossy(data).to_uppercase()
            }
            Value::SimpleString(s) => s.to_uppercase(),
            _ => return Err(ProtocolError::InvalidArgument("field").into()),
        };

        let value = match &items[3] {
            Value::BulkString(Some(data)) => {
                String::from_utf8_lossy(data).to_string()
            }
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("value").into()),
        };

        match field.as_str() {
            "LIB-NAME" => {
                client.lib_name = value;
            }
            "LIB-VER" => {
                client.lib_ver = value;
            }
            _ => {
                return Err(
                    ProtocolError::InvalidArgument("SETINFO sub-option").into()
                );
            }
        }

        Ok(Value::ok())
    }
}