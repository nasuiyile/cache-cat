use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::AppendReq;
use crate::raft::types::entry::bae_operation::BaseOperation::Append;
use crate::raft::types::entry::request::RedisOperation::RedisSet;
use crate::raft::types::entry::request::Request;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

/// Parameters for APPEND command
#[derive(Debug, Clone, PartialEq)]
pub struct AppendParams {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl AppendParams {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("APPEND"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let value = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("value")),
        };

        Ok(AppendParams::new(key, value))
    }
}

/// APPEND command executor
pub struct AppendCommand;

#[async_trait]
impl Command for AppendCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = AppendParams::parse(items)?;
        let operation = Append(AppendReq {
            key: Arc::from(params.key),
            value: Arc::from(params.value),
        });
        let value = server.app.write_base(operation, *db_number).await?;
        Ok(value)
    }
}
