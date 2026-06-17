use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::lua::eval::EvalParams;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisEval;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for EVALSHA command
///
/// Standard Redis EVALSHA command format:
/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalShaParams {
    /// SHA1 hash of the Lua script
    pub sha1: Bytes,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
}

impl Display for EvalShaParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sha1 = self.sha1().ok_or(std::fmt::Error)?;
        let sha1 = if self.sha1.len() > 20 {
            format_args!("{}...", &sha1[..20])
        } else {
            format_args!("{}", sha1)
        };
        write!(
            f,
            "EVALSHA {} ({} keys, {} args)",
            sha1,
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalShaParams {
    /// Create a new EvalShaParams
    pub fn new(sha1: Bytes, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
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
        let sha1 = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("sha1"))?
            .clone();

        // Parse numkeys
        let numkeys = items[2].parse_usize().ok_or(ProtocolError::NotAnInteger)?;

        // Validate key count
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

        // Parse args
        let mut args = Vec::new();

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

        Ok(EvalShaParams::new(sha1, numkeys, keys, args))
    }

    #[inline]
    pub fn sha1(&self) -> Option<&str> {
        str::from_utf8(&self.sha1).ok()
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
