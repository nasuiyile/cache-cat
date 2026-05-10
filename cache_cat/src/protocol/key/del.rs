//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Del;
use crate::raft::types::entry::bae_operation::DelReq;
use crate::raft::types::entry::request::RedisOperation::RedisDel;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// DEL command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DelParams {
    pub keys: Vec<Vec<u8>>,
}

impl DelParams {
    /// Parse DEL command parameters from RESP array items
    /// Format: DEL key [key ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: DEL key (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("del"));
        }

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(items.len() - 1);
        for item in items.iter().skip(1) {
            let key = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::WrongArgCount("del")),
            };
            keys.push(key);
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

#[async_trait]
impl Command for DelCommand {
    async fn execute(
        &self,
        db_number: &mut u16,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = DelParams::parse(items)?;
        let value = if params.keys.len() == 1 {
            let operation = Del(DelReq {
                key: Arc::new(params.keys[0].clone()),
            });
            server.app.write_base(operation, *db_number).await?
        } else {
            server.app.write_redis(RedisDel(params), *db_number).await?
        };
        Ok(value)
    }
}
