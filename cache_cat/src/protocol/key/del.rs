//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Del;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

/// DEL command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DelParams {
    pub keys: Vec<Bytes>,
}

impl DelParams {
    /// Parse DEL command parameters from RESP array items
    /// Format: DEL key [key ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: DEL key (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("del"));
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 1 {
            return Err(ProtocolError::WrongArgCount("del"));
        }

        Ok(DelParams { keys })
    }
}
impl Display for DelParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelReq {{ keys: {:?} }}", self.keys)
    }
}

/// DEL command executor
pub struct DelCommand;

impl RaftCommand for DelCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = DelParams::parse(items)?;
        let operation = if params.keys.len() == 1 {
            Operation::Base(Del(DelReq {
                key: params.keys[0].clone(),
            }))
        } else {
            cfg_select! {
                feature = "redis" => {
                    use crate::raft::types::entry::request::RedisOperation::RedisDel;

                    Operation::Redis(RedisDel(params))
                }

                _ => {
                    return Err(ProtocolError::InvalidArgument("command of `redis` is not enabled"));
                }
            }
        };
        Ok(operation)
    }
}

#[async_trait]
impl Command for DelCommand {
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DelReq {
    pub key: Bytes,
}

impl Display for DelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DelReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
