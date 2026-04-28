use crate::error::{CacheCatError, CacheCatResult, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::rpc::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::utils::now_ms;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

/// Parameters for MGET command
#[derive(Debug, Clone, PartialEq)]
pub struct MgetParams {
    pub keys: Vec<Vec<u8>>,
}

impl MgetParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("MGET"));
        }

        let mut keys = Vec::with_capacity(items.len() - 1);
        for item in &items[1..] {
            let key = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("key")),
            };
            keys.push(key);
        }

        Ok(MgetParams { keys })
    }
}

/// Get a value from the server
/// Returns None if key not found or expired.
async fn get_value(server: &RedisServer, key: &Vec<u8>) -> CacheCatResult<Option<Vec<u8>>> {
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

/// MGET command executor
pub struct MgetCommand;

#[async_trait]
impl Command for MgetCommand {
    async fn execute(
        &self,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = MgetParams::parse(items)?;
        let raft = &server.app.raft;
        let linearizer = raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        let _lock = server.app.state_machine.data.kvs.batch_lock.lock();
        let mut results = Vec::with_capacity(params.keys.len());

        for key in &params.keys {
            match get_value(server, key).await {
                Ok(Some(value)) => {
                    results.push(Value::BulkString(Some(value)));
                }
                Ok(None) => {
                    results.push(Value::BulkString(None));
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(Value::Array(Some(results)))
    }
}
