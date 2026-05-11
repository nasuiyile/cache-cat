use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use std::sync::Arc;

/// Parameters for MGET command
#[derive(Debug, Clone, PartialEq)]
pub struct WatchParams {
    pub keys: Vec<Vec<u8>>,
}

impl WatchParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("WATCH"));
        }

        let mut keys = Vec::with_capacity(items.len() - 1);
        for item in &items[1..] {
            let key = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("key")),
            };
            keys.push(key);
        }

        Ok(WatchParams { keys })
    }
}
/// WATCH command executor
pub struct WatchCommand;
#[async_trait]
impl Command for WatchCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = WatchParams::parse(items)?;

        Ok(Value::ok())
    }
}
