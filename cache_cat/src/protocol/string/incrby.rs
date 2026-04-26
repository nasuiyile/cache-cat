use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::rpc::RedisServer;
use std::sync::Arc;

use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Incr;
use crate::raft::types::entry::bae_operation::IncrReq;
use crate::raft::types::entry::request::Request;
use crate::utils::now_ms;
use async_trait::async_trait;

/// Parameters for INCR command
#[derive(Debug, Clone, PartialEq)]
pub struct IncrByParams {
    pub key: Vec<u8>,
    pub increment: i64,
}

impl IncrByParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("INCR"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };
        let increment = match &items[2] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<i64>()
                    .map_err(|_| ProtocolError::InvalidArgument("increment"))?
            }
            Value::SimpleString(s) => s
                .parse::<i64>()
                .map_err(|_| ProtocolError::InvalidArgument("increment"))?,
            Value::Integer(i) => *i,
            _ => return Err(ProtocolError::InvalidArgument("increment")),
        };

        Ok(IncrByParams { key, increment })
    }
}

/// INCR command executor
pub struct IncrByCommand;

#[async_trait]
impl Command for IncrByCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        let params = IncrByParams::parse(items)?;
        let req = IncrReq {
            key: Arc::from(params.key),
            value: params.increment,
        };
        let res = server
            .app
            .raft
            .client_write(Request::Base(Incr(req)))
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        match res.data {
            Value::Integer(i) => Ok(Value::Integer(i)),
            _ => Err(CacheCatError::from(StorageError::WriteFailed(
                "ERR unexpected response".to_string(),
            ))),
        }
    }
}
