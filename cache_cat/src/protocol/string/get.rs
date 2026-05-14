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

/// Parameters for GET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetParams {
    pub key: Vec<u8>,
}
impl Display for GetParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GET {}", String::from_utf8_lossy(&self.key))
    }
}

impl GetParams {
    /// Parse GET command parameters from RESP array items
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("GET"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(GetParams { key })
    }
}

/// GET command executor
pub struct GetCommand;

impl RaftCommand for GetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = GetParams::parse(items)?;
        Ok(Operation::Read(ReadOperation::Get(params)))
    }
}

#[async_trait]
impl Command for GetCommand {
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

        let params = GetParams::parse(items)?;
        let values = server.app.read(params.key, client.db_number).await?;
        match values {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::Int(int_value) => {
                    Ok(Value::BulkString(Some(int_value.to_string().into_bytes())))
                }
                ValueObject::String(str_value) => {
                    Ok(Value::BulkString(Some(str_value.as_ref().clone())))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
