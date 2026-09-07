use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use crate::mocha::EntrySnapshot;

pub struct SMembersCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SMembersParams {
    pub key: Bytes,
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

impl ReadCommand for SMembersParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // Missing key: empty set reply (RESP2 *0, RESP3 ~0), like Redis.
            None => Value::Set(Vec::new()),
            Some(v) => match v.value.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();

                    let members = guard
                        .iter()
                        .map(|v| Value::BulkString(Some(v.clone())))
                        .collect::<Vec<_>>();

                    Value::Set(members)
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl SMembersCommand {
    fn parse_args(items: &[Value]) -> Result<SMembersParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("smembers"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(SMembersParams { key })
    }
}

impl ReadRaftCommand for SMembersCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SMembers(SMembersCommand::parse_args(items)?))
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
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}
