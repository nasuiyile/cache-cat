use crate::error::{CacheCatError, CacheCatResult, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::moka::moka::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

/// Parameters for GET command
#[derive(Debug, Clone, PartialEq)]
pub struct GetParams {
    pub key: Vec<u8>,
}

impl GetParams {
    /// Parse GET command parameters from RESP array items
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("GET"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(GetParams { key })
    }
}

/// GET command executor
pub struct GetCommand;

#[async_trait]
impl Command for GetCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = GetParams::parse(items)?;
        let values = server.app.read(params.key, *db_number).await?;
        match values {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::Int(int_value) => {
                    Ok(Value::BulkString(Some(int_value.to_string().into_bytes())))
                }
                ValueObject::String(str_value) => {
                    Ok(Value::BulkString(Some(str_value.as_ref().clone())))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
