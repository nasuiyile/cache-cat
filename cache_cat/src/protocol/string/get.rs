use crate::error::{CacheCatError, CacheCatResult, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::rpc::{RedisServer, Server};
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

/// Get a value from the server, checking for expiration.
/// Returns (value, expired) where `expired` is true if the key was expired and deleted.
async fn get_value_check_expiry(
    server: &RedisServer,
    key: &Vec<u8>,
) -> CacheCatResult<Option<Vec<u8>>> {
    let raft = &server.app.raft;
    let linearizer = raft
        .get_read_linearizer(LeaseRead)
        .await
        .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
    linearizer.await_ready(&raft).await.unwrap();
    let value = server.app.state_machine.data.kvs.cache.get(key).await;
    match value {
        None => Ok(None),
        Some(v) => match v.data {
            ValueObject::Int(int_value) => {
                //转换为字符串
                Ok(Some(int_value.to_string().into_bytes()))
            }
            ValueObject::String(string_value) => Ok(Some(string_value.as_ref().clone())),
            _ => Err(CacheCatError::from(ProtocolError::WrongType)),
        },
    }
}

/// GET command executor
pub struct GetCommand;

#[async_trait]
impl Command for GetCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        let params = GetParams::parse(items)?;

        match get_value_check_expiry(server, &params.key).await? {
            Some(data) => Ok(Value::BulkString(Some(data))),
            None => Ok(Value::BulkString(None)), // Key not found or expired
        }
    }
}
