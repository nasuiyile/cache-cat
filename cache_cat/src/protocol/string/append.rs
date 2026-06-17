use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::AppendReq;
use crate::raft::types::entry::bae_operation::BaseOperation::Append;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// Parameters for APPEND command
#[derive(Debug, Clone, PartialEq)]
pub struct AppendParams {
    pub key: Bytes,
    pub value: Bytes,
}

impl AppendParams {
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("APPEND"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let value = items[2]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("value"))?
            .clone();

        Ok(AppendParams::new(key, value))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.value) }
    }
}

impl RaftCommand for AppendCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = AppendParams::parse(items)?;
        Ok(Operation::Base(Append(AppendReq {
            key: params.key,
            value: params.value,
        })))
    }
}

/// APPEND command executor
pub struct AppendCommand;

#[async_trait]
impl Command for AppendCommand {
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
