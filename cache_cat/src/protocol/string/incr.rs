use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use std::sync::Arc;

use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Incr;
use crate::raft::types::entry::bae_operation::IncrReq;
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
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = IncrParams::parse(items)?;
        let operation = Incr(IncrReq {
            key: Arc::from(params.key),
            value: 1,
        });
        let value = server.app.write_base(operation, *db_number).await?;
        Ok(value)
    }
}
