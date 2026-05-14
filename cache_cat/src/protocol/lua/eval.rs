use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisEval;
use async_trait::async_trait;
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
    pub keys: Vec<Vec<u8>>,
    /// Arguments
    pub args: Vec<Vec<u8>>,
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
    pub fn new(script: String, numkeys: usize, keys: Vec<Vec<u8>>, args: Vec<Vec<u8>>) -> Self {
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
        let script = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("script")),
        };

        // Parse numkeys
        let numkeys = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data)
                .parse::<usize>()
                .map_err(|_| ProtocolError::NotAnInteger)?,
            Value::SimpleString(s) => s
                .parse::<usize>()
                .map_err(|_| ProtocolError::NotAnInteger)?,
            Value::Integer(i) if *i >= 0 => *i as usize,
            _ => return Err(ProtocolError::NotAnInteger),
        };

        // Expected total items: 3 (header) + numkeys + numargs
        // Actual remaining items after script and numkeys: items.len() - 3
        let remaining = items.len() - 3;
        if remaining < numkeys {
            return Err(ProtocolError::InvalidArgument(
                "not enough keys specified",
            ));
        }

        // Parse keys
        let mut keys = Vec::with_capacity(numkeys);
        for i in 0..numkeys {
            let key_value = &items[3 + i];
            let key = match key_value {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("key")),
            };
            keys.push(key);
        }

        // Parse arguments (remaining items after keys)
        let mut args = Vec::new();
        for i in (3 + numkeys)..items.len() {
            let arg_value = &items[i];
            let arg = match arg_value {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                Value::Integer(i) => i.to_string().as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("argument")),
            };
            args.push(arg);
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
        let params = EvalParams::parse(items)?;
        let result = server
            .app
            .write(Operation::Redis(RedisEval(params)), client.db_number)
            .await?;
        Ok(result)
    }
}