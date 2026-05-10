use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::SAdd;
use crate::raft::types::entry::bae_operation::SAddReq;
use async_trait::async_trait;
use std::sync::Arc;

struct SAddArgs {
    key: Vec<u8>,
    members: Vec<Vec<u8>>,
}

pub struct SAddCommand;

impl SAddCommand {
    fn parse_args(items: &[Value]) -> Result<SAddArgs, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("sadd"));
        }

        // Parse key
        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let mut members = Vec::with_capacity(items.len() - 2);
        for item in &items[2..] {
            let member = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("member")),
            };
            members.push(member);
        }

        Ok(SAddArgs { key, members })
    }
}

#[async_trait]
impl Command for SAddCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = Self::parse_args(items)?;
        let mut elements = Vec::new();
        for v in params.members {
            elements.push(Arc::new(v));
        }
        let operation = SAdd(SAddReq {
            key: Arc::from(params.key),
            elements,
        });
        let value = server.app.write_base(operation, *db_number).await?;
        Ok(value)
    }
}
