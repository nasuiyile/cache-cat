use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisSetNx;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetNxParams {
    /// The key to set
    pub key: Bytes,
    /// The value to set
    pub value: Bytes,
}

impl fmt::Display for SetNxParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SetNx {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

/// SETNX command executor
pub struct SetNxCommand;

impl SetNxCommand {
    /// Parse SETNX command parameters
    ///
    /// Format:
    /// SETNX key value
    fn parse(items: &[Value]) -> Result<SetNxParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("setnx"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let value = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(SetNxParams { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(value: &'static [u8]) -> Value {
        Value::BulkString(Some(Bytes::from_static(value)))
    }

    #[test]
    fn parse_uses_the_value_argument() {
        let params = SetNxCommand::parse(&[bulk(b"SETNX"), bulk(b"key"), bulk(b"value")])
            .expect("valid SETNX arguments");

        assert_eq!(params.key, Bytes::from_static(b"key"));
        assert_eq!(params.value, Bytes::from_static(b"value"));
    }

    #[test]
    fn parse_rejects_non_string_value() {
        let result = SetNxCommand::parse(&[bulk(b"SETNX"), bulk(b"key"), Value::Integer(1)]);

        assert_eq!(result, Err(ProtocolError::InvalidArgument("value")));
    }
}

impl RaftCommand for SetNxCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Redis(RedisSetNx(params)))
    }
}

#[async_trait]
impl Command for SetNxCommand {
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
            .write(Operation::Redis(RedisSetNx(params)), client.db_number)
            .await
    }
}
