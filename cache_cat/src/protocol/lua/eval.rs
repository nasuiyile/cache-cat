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
    pub script: Bytes,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
}

impl Display for EvalParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let script = self.script();
        let script = if self.script.len() > 20 {
            format_args!("{}...", &script[..20])
        } else {
            format_args!("{}", script)
        };
        write!(
            f,
            "EVAL {} ({} keys, {} args)",
            script,
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalParams {
    /// Create a new EvalParams
    pub fn new(script: Bytes, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
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
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("script"))?
            .clone();

        // Parse numkeys
        let numkeys = items[2].parse_usize().ok_or(ProtocolError::NotAnInteger)?;

        // Expected total items: 3 (header) + numkeys + numargs
        // Actual remaining items after script and numkeys: items.len() - 3
        let remaining = items.len() - 3;
        if remaining < numkeys {
            return Err(ProtocolError::InvalidArgument("not enough keys specified"));
        }

        // Parse keys
        let mut keys = Vec::with_capacity(numkeys);
        for i in 0..numkeys {
            let key = items[3 + i]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("key"))?
                .clone();

            keys.push(key);
        }

        // Parse arguments (remaining items after keys)
        let mut args = Vec::with_capacity(items.len() - numkeys - 3);
        for i in (3 + numkeys)..items.len() {
            let arg = match &items[i] {
                Value::BulkString(Some(data)) => Some(data.clone()),
                Value::SimpleString(s) => Some(s.clone()),
                Value::Integer(i) => Some(i.to_string().into()),
                _ => None,
            }
            .ok_or(ProtocolError::InvalidArgument("argument"))?;

            args.push(arg);
        }

        Ok(EvalParams::new(script, numkeys, keys, args))
    }

    #[inline]
    pub fn script(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.script) }
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
            return Ok(Value::from_static_string("QUEUED"));
        }
        let operation = self.raft_request(items)?;
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}
