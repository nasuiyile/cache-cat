use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::SAdd;
use crate::raft::types::entry::bae_operation::SAddReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

struct SAddArgs {
    key: Bytes,
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

        Ok(SAddArgs {
            key: key.into(),
            members,
        })
    }
}

impl RaftCommand for SAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        let mut elements = Vec::new();
        for v in params.members {
            elements.push(Arc::new(v));
        }
        Ok(Operation::Base(SAdd(SAddReq {
            key: params.key,
            elements,
        })))
    }
}

#[async_trait]
impl Command for SAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
