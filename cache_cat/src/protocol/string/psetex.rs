use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisPSetEx;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PSetExParams {
    /// The key to set
    pub key: Bytes,
    /// The value to set
    pub value: Bytes,
    /// Expiration time
    pub expiration: u64,
}

impl fmt::Display for PSetExParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PSetEx {} {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.expiration
        )
    }
}

pub struct PSetExCommand;

impl PSetExCommand {
    fn parse(items: &[Value]) -> Result<PSetExParams, ProtocolError> {
        // PSETEX key milliseconds value
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("psetex"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let milliseconds = items[2].try_parse_u64()?;

        let value = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(PSetExParams {
            key,
            value,
            expiration: milliseconds,
        })
    }
}

impl RaftCommand for PSetExCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Redis(RedisPSetEx(params)))
    }
}

#[async_trait]
impl Command for PSetExCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let params = Self::parse(items)?;
        server
            .app
            .write(Operation::Redis(RedisPSetEx(params)), client.db_number)
            .await?;

        Ok(Value::ok())
    }
}
