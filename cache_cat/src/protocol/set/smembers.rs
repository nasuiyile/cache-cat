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

pub struct SMembersCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMembersParams {
    pub key: Vec<u8>,
}

impl Display for SMembersParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SMembersParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl SMembersCommand {
    fn parse_args(items: &[Value]) -> Result<SMembersParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("smembers"));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(SMembersParams { key })
    }
}

impl RaftCommand for SMembersCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = SMembersCommand::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::SMembers(params)))
    }
}

#[async_trait]
impl Command for SMembersCommand {
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
        let params = SMembersCommand::parse_args(items)?;
        let my_value = server.app.read(params.key, client.db_number).await?;
        match my_value {
            None => Ok(Value::Array(Some(vec![]))),
            Some(v) => match v.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();
                    let mut array = Vec::new();
                    for member in guard.iter() {
                        array.push(Value::BulkString(Some(member.as_ref().clone())));
                    }
                    Ok(Value::Array(Some(array)))
                }
                _ => Err(ProtocolError::WrongType.into()),
            },
        }
    }
}
