use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisEval;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for EVALSHA command
///
/// Standard Redis EVALSHA command format:
/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalShaParams {
    /// SHA1 hash of the Lua script
    pub sha1: String,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Vec<u8>>,
    /// Arguments
    pub args: Vec<Vec<u8>>,
}

impl Display for EvalShaParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EVALSHA {} ({} keys, {} args)",
            if self.sha1.len() > 20 {
                format!("{}...", &self.sha1[..20])
            } else {
                self.sha1.clone()
            },
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalShaParams {
    /// Create a new EvalShaParams
    pub fn new(sha1: String, numkeys: usize, keys: Vec<Vec<u8>>, args: Vec<Vec<u8>>) -> Self {
        Self {
            sha1,
            numkeys,
            keys,
            args,
        }
    }

    /// Parse EVALSHA command parameters from RESP array items
    /// Format: EVALSHA sha1 numkeys key [key ...] arg [arg ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: EVALSHA sha1 numkeys
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("evalsha"));
        }

        // Parse sha1
        let sha1 = match &items[1] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("sha1")),
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

        // Validate key count
        let remaining = items.len() - 3;
        if remaining < numkeys {
            return Err(ProtocolError::InvalidArgument("not enough keys specified"));
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

        // Parse args
        let mut args = Vec::new();

        for i in (3 + numkeys)..items.len() {
            let arg_value = &items[i];

            let arg = match arg_value {
                Value::BulkString(Some(data)) => data.clone(),

                Value::SimpleString(s) => s.as_bytes().to_vec(),

                Value::Integer(i) => i.to_string().into_bytes(),

                _ => return Err(ProtocolError::InvalidArgument("argument")),
            };

            args.push(arg);
        }

        Ok(EvalShaParams::new(sha1, numkeys, keys, args))
    }
}

/// EVALSHA command executor
pub struct EvalShaCommand;

#[async_trait]
impl Command for EvalShaCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // MULTI transaction support
        let params = EvalShaParams::parse(items)?;
        let script_result = match server
            .app
            .state_machine
            .data
            .kvs
            .lua_env
            .script_map
            .lock()
            .get(&params.sha1)
        {
            None => {
                return Err(
                    ProtocolError::Custom("NOSCRIPT No matching script. Please use EVAL.").into(),
                );
            }
            Some(v) => v.clone(),
        };
        let operation = Operation::Redis(RedisEval(EvalParams {
            script: script_result.clone(),
            keys: params.keys,
            args: params.args,
            numkeys: params.numkeys,
        }));
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}
