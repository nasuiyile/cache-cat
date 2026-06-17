use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Incr;
use crate::raft::types::entry::bae_operation::IncrReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// Parameters for INCR command
#[derive(Debug, Clone, PartialEq)]
pub struct IncrByParams {
    pub key: Bytes,
    pub increment: i64,
}

impl IncrByParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("INCR"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let increment = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("increment"))?;

        Ok(IncrByParams { key, increment })
    }
}

/// INCR command executor
pub struct IncrByCommand;

impl RaftCommand for IncrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = IncrByParams::parse(items)?;
        Ok(Operation::Base(Incr(IncrReq {
            key: params.key,
            value: params.increment,
        })))
    }
}

#[async_trait]
impl Command for IncrByCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
