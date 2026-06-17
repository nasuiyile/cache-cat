use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parameters for MSET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsetParams {
    pub pairs: Vec<(Bytes, Bytes)>,
}

impl MsetParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("MSET"));
        }

        let args_count = items.len() - 1;
        if !args_count.is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("MSET"));
        }

        let mut pairs = Vec::with_capacity(args_count / 2);
        let mut i = 1;
        while i < items.len() {
            let key = items[i]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("key"))?
                .clone();

            let value = items[i + 1]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("value"))?
                .clone();

            pairs.push((key, value));
            i += 2;
        }

        Ok(MsetParams { pairs })
    }
}
impl Display for MsetParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MSET {{ pairs: {:?} }}", self.pairs)
    }
}

impl RaftCommand for MsetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Redis(RedisOperation::RedisMset(
            MsetParams::parse(items)?,
        )))
    }
}

/// MSET command executor
pub struct MsetCommand;

#[async_trait]
impl Command for MsetCommand {
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
