///! FLUSHDB command implementation
///
/// FLUSHDB [ASYNC | SYNC]
/// Delete all the keys of the currently selected DB.
///
/// Returns:
/// - Simple string OK

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::FlushDB;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

/// FLUSHDB command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlushDBParams {
    pub async_mode: bool,
}

impl FlushDBParams {
    /// Parse FLUSHDB command parameters from RESP array items
    /// Format: FLUSHDB [ASYNC | SYNC]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // FLUSHDB can be called with 1 or 2 items
        if items.is_empty() || items.len() > 2 {
            return Err(ProtocolError::WrongArgCount("flushdb"));
        }

        let mut async_mode = false;

        // Parse optional argument
        if items.len() == 2 {
            if let Some(arg) = items[1].string_bytes_clone() {
                let arg_upper = String::from_utf8_lossy(&arg).to_uppercase();
                match arg_upper.as_str() {
                    "ASYNC" => async_mode = true,
                    "SYNC" => async_mode = false,
                    _ => return Err(ProtocolError::SyntaxError),
                }
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }

        Ok(FlushDBParams { async_mode })
    }
}

impl Display for FlushDBParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushDBReq {{ async_mode: {} }}",
            self.async_mode
        )
    }
}

/// FLUSHDB command executor
pub struct FlushDBCommand;

impl RaftCommand for FlushDBCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = FlushDBParams::parse(items)?;
        Ok(Operation::Base(FlushDB(FlushDBReq {
            async_mode: params.async_mode,
        })))
    }
}

#[async_trait]
impl Command for FlushDBCommand {
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
pub struct FlushDBReq {
    pub async_mode: bool,
}

impl Display for FlushDBReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FlushDBReq {{ async_mode: {} }}",
            self.async_mode
        )
    }
}