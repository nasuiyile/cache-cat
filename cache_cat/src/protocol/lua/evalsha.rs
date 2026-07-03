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
    pub sha1: String,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
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
    pub fn new(sha1: String, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
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
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("sha1"))?
            .into_owned();

        // Parse numkeys
        let numkeys = items[2].try_parse_usize()?;

        // Validate key count
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
        // Parse args
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
