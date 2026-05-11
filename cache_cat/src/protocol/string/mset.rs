use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::RedisOperation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parameters for MSET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsetParams {
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MsetParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("MSET"));
        }

        let args_count = items.len() - 1;
        if !args_count.is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("MSET"));
        }

        let mut pairs = Vec::with_capacity(args_count / 2);
        let mut i = 1;
        while i < items.len() {
            let key = match &items[i] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("key")),
            };

            let value = match &items[i + 1] {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::InvalidArgument("value")),
            };

            pairs.push((key, value));
            i += 2;
        }

        Ok(MsetParams { pairs })
    }
}
impl Display for MsetParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MSET {{ pairs: {:?} }}", self.pairs)
    }
}

/// MSET command executor
pub struct MsetCommand;

#[async_trait]
impl Command for MsetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = MsetParams::parse(items)?;
        let operation = RedisOperation::RedisMset(params);
        let value = server.app.write_redis(operation, client.db_number).await?;
        Ok(value)
    }
}
