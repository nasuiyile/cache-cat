use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BfCardParams {
    pub key: Bytes,
}

impl Display for BfCardParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BF.CARD {}", String::from_utf8_lossy(&self.key))
    }
}

impl BfCardParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("BF.CARD"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(Self { key })
    }
}

pub struct BfCardCommand;

impl ReadCommand for BfCardParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::Integer(0),

            Some(entry) => match &entry.value.data {
                ValueObject::Bloom(bloom) => {
                    let bloom = bloom.lock();

                    match i64::try_from(bloom.info_items()) {
                        Ok(value) => Value::Integer(value),
                        Err(_) => ProtocolError::Overflow.into(),
                    }
                }

                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl ReadRaftCommand for BfCardCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BfCard(BfCardParams::parse(items)?))
    }
}

#[async_trait]
impl Command for BfCardCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::SimpleString("QUEUED".to_string()));
        }

        let operation = self.read_operation(items)?;
        server.app.read(operation, client.db_number).await
    }
}
