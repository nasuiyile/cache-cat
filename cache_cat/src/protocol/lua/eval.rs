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
    /// RESP protocol version of the calling client (2 or 3). Controls the
    /// Lua -> RESP conversion of the script's return value, like Redis.
    #[serde(default = "default_eval_proto")]
    pub proto: u8,
}

#[inline]
pub(crate) fn default_eval_proto() -> u8 {
    2
}

impl Display for EvalParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EVAL {}{} ({} keys, {} args)",
            String::from_utf8_lossy(&self.script[..self.script.len().min(20)]),
            if self.script.len() > 20 { "..." } else { "" },
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
            proto: default_eval_proto(),
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
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("script"))?;

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
            return Ok(Value::queued());
        }
        let mut operation = self.raft_request(items)?;
        if let Operation::Redis(RedisEval(ref mut params)) = operation {
            params.proto = client.framed.codec().proto_version();
        }
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::do_request;

    fn execute(cache: &MyCache, parts: &[&[u8]]) -> Value {
        let items: Vec<_> = parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part))))
            .collect();
        let operation = EvalCommand.raft_request(&items).unwrap();
        // Exercise the same serialization used for replicated operations.
        let encoded = bincode2::serialize(&operation).unwrap();
        let operation: Operation = bincode2::deserialize(&encoded).unwrap();
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: 0,
            update_type: &mut update_type,
        };
        do_request(cache, operation, &mut update, true)
    }

    #[test]
    fn lua_calls_preserve_binary_keys_and_values() {
        let cache = MyCache::new(1).unwrap();
        let key = b"key\xff\0\r\n";
        let value = b"value\xfe\0\r\n";
        for function in ["call", "pcall"] {
            let script = format!(
                "redis.{function}('SET', KEYS[1], ARGV[1]); return redis.{function}('GET', KEYS[1])"
            );
            assert_eq!(
                execute(&cache, &[b"EVAL", script.as_bytes(), b"1", key, value]).encode(),
                Value::BulkString(Some(Bytes::copy_from_slice(value))).encode()
            );
            // Lua numbers must still be accepted as command arguments.
            let script = format!(
                "redis.{function}('SET', KEYS[1], 123); return redis.{function}('GET', KEYS[1])"
            );
            assert_eq!(
                execute(&cache, &[b"EVAL", script.as_bytes(), b"1", key]).encode(),
                b"$3\r\n123\r\n"
            );
        }
        assert_eq!(
            execute(&cache, &[b"EVAL", b"return ARGV[1]", b"0", value]).encode(),
            Value::BulkString(Some(Bytes::copy_from_slice(value))).encode()
        );
    }

    #[test]
    fn eval_preserves_binary_source_in_replication_and_compiled_cache() {
        let cache = MyCache::new(1).unwrap();
        for byte in [0xff, 0xfe, 0xff] {
            let mut script = b"return \"".to_vec();
            script.extend_from_slice(&[byte, b'"']);
            assert_eq!(
                execute(&cache, &[b"EVAL", &script, b"0"]).encode(),
                vec![b'$', b'1', b'\r', b'\n', byte, b'\r', b'\n']
            );
        }
    }

    #[test]
    fn eval_rejects_precompiled_lua_bytecode() {
        let cache = MyCache::new(1).unwrap();
        let bytecode = mlua::Lua::new()
            .load("return 123")
            .into_function()
            .unwrap()
            .dump(false);
        assert!(matches!(
            execute(&cache, &[b"EVAL", &bytecode, b"0"]),
            Value::Error(_)
        ));
    }
}
