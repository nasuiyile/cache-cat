use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use std::sync::Arc;

use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Incr;
use crate::raft::types::entry::bae_operation::IncrReq;
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
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = IncrByParams::parse(items)?;
        let operation = Incr(IncrReq {
            key: Arc::from(params.key),
            value: params.increment,
        });
        let value = server.app.write_base(operation, client.db_number).await?;
        Ok(value)
    }
}
