use crate::protocol::command::Command;
use crate::raft::network::rpc::{RedisServer, Server};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::raft_types::get_group_by_key;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

/// Parameters for GET command
#[derive(Debug, Clone, PartialEq)]
pub struct GetParams {
    pub key: Vec<u8>,
}

impl GetParams {
    /// Parse GET command parameters from RESP array items
    fn parse(items: &[Value]) -> Option<Self> {
        if items.len() != 2 {
            return None;
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return None,
        };

        Some(GetParams { key })
    }
}

/// Get a value from the server, checking for expiration.
/// Returns (value, expired) where `expired` is true if the key was expired and deleted.
async fn get_value_check_expiry(server: &RedisServer, key: &Vec<u8>) -> Result<Option<Vec<u8>>, String> {
    let group = get_group_by_key(&server.app, key);
    let ret = group.raft.get_read_linearizer(LeaseRead).await;

    let value = match ret {
        Ok(linearizer) => {
            linearizer.await_ready(&group.raft).await.unwrap();
            group.state_machine.data.kvs.cache.get(key).await
        }
        Err(_) => return Err("corrupted value".to_string()),
    };
    match value {
        None => Ok(None),
        Some(v) => match v.data {
            ValueObject::Int(int_value) => {
                //转换为字符串
                Ok(Some(int_value.to_string().into_bytes()))
            }
            ValueObject::String(string_value) => Ok(Some(string_value.as_ref().clone())),
            _ => Err("corrupted value".to_string()),
        },
    }
}

/// GET command executor
pub struct GetCommand;

#[async_trait]
impl Command for GetCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Value {
        let params = match GetParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'get' command"),
        };

        match get_value_check_expiry(server, &params.key).await {
            Ok(Some(data)) => Value::BulkString(Some(data)),
            Ok(None) => Value::BulkString(None), // Key not found or expired
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}
