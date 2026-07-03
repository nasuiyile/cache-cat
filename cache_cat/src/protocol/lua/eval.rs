use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisEval;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for EVAL command
///
/// Standard Redis EVAL command format:
/// EVAL script numkeys key [key ...] arg [arg ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalParams {
    /// The Lua script to execute
    pub script: String,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
}

impl Display for EvalParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EVAL {} ({} keys, {} args)",
            if self.script.len() > 20 {
                format!("{}...", &self.script[..20])
            } else {
                self.script.clone()
            },
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalParams {
    /// Create a new EvalParams
    pub fn new(script: String, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
        Self {
            script,
            numkeys,
            keys,
            args,
        }
    }

    /// Parse EVAL command parameters from RESP array items
    /// Format: EVAL script numkeys key [key ...] arg [arg ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: EVAL script numkeys
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("eval"));
        }

        // Parse script
        let script = items[1]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("script"))?
            .into_owned();

        // Parse numkeys
        let numkeys = items[2].try_parse_usize()?;

        // Expected total items: 3 (header) + numkeys + numargs
        // Actual remaining items after script and numkeys: items.len() - 3
        let remaining = items.len() - 3;
        if remaining < numkeys {
            return Err(ProtocolError::InvalidArgument("not enough keys specified"));
        }

        // Parse keys
        let keys = items[3..3 + numkeys]
            .iter()
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < numkeys {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        let start = 3 + numkeys;
        // Parse arguments (remaining items after keys)
        let args = items
            .iter()
            .skip(start)
            .map_while(|arg_value| match arg_value {
                Value::BulkString(Some(data)) => Some(data.clone()),
                Value::SimpleString(s) => Some(s.clone().into()),
                Value::Integer(i) => Some(i.to_string().into()),
                _ => None,
            })
            .collect::<Vec<_>>();

        if args.len() < items.len() - start {
            return Err(ProtocolError::InvalidArgument("argument"));
        }

        Ok(EvalParams::new(script, numkeys, keys, args))
    }
}

/// EVAL command executor
pub struct EvalCommand;

impl RaftCommand for EvalCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = EvalParams::parse(items)?;
        Ok(Operation::Redis(RedisEval(params)))
    }
}

#[async_trait]
impl Command for EvalCommand {
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
        let operation = self.raft_request(items)?;
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}
