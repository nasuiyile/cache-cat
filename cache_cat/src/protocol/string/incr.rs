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
pub struct IncrParams {
    pub key: Vec<u8>,
}

impl IncrParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("INCR"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(IncrParams { key })
    }
}

/// INCR command executor
pub struct IncrCommand;

#[async_trait]
impl Command for IncrCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        let params = IncrParams::parse(items)?;
        let req = IncrReq {
            key: Arc::from(params.key),
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
