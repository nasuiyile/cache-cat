use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for MGET command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgetParams {
    pub keys: Vec<Vec<u8>>,
}
impl Display for MgetParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MGET")?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl MgetParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("MGET"));
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

        Ok(MgetParams { keys })
    }
}
/// MGET command executor
pub struct MgetCommand;

impl RaftCommand for MgetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = MgetParams::parse(items)?;
        Ok(Operation::Read(ReadOperation::MGet(params)))
    }
}

#[async_trait]
impl Command for MgetCommand {
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
        let params = MgetParams::parse(items)?;
        let values = server.app.multi_read(params.keys, client.db_number).await?;
        let mut results = Vec::with_capacity(values.len());
        for my_value in values {
            match my_value {
                None => {
                    results.push(Value::BulkString(None));
                }
                Some(v) => match v.data {
                    ValueObject::Int(int_value) => {
                        results.push(Value::BulkString(Some(int_value.to_string().into_bytes())));
                    }
                    ValueObject::String(str_value) => {
                        results.push(Value::BulkString(Some(str_value.as_ref().clone())));
                    }
                    _ => return Err(CacheCatError::from(ProtocolError::WrongType)),
                },
            }
        }
        Ok(Value::Array(Some(results)))
    }
}
