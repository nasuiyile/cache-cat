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

/// Parameters for BF.MEXISTS command
///
/// BF.MEXISTS key item [item ...]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BfMExistsParams {
    pub key: Bytes,
    pub items: Vec<Bytes>,
}
impl Display for BfMExistsParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BF.MEXISTS {}", String::from_utf8_lossy(&self.key))?;
        for item in &self.items {
            write!(f, " {}", String::from_utf8_lossy(item))?;
        }
        Ok(())
    }
}

impl BfMExistsParams {
    fn parse(values: &[Value]) -> Result<Self, ProtocolError> {
        if values.len() < 3 {
            return Err(ProtocolError::WrongArgCount("BF.MEXISTS"));
        }
        let key = values[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let items = values
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();
        if items.len() < values.len() - 2 {
            return Err(ProtocolError::InvalidArgument("item"));
        }
        Ok(Self { key, items })
    }
}

/// BF.MEXISTS command executor
pub struct BfMExistsCommand;

impl ReadCommand for BfMExistsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let mut results = Vec::with_capacity(self.items.len());
        match value {
            None => {
                results.resize(self.items.len(), Value::Boolean(false));
            }
            Some(entry) => match &entry.value.data {
                ValueObject::Bloom(bloom) => {
                    let bloom = bloom.lock();
                    for item in &self.items {
                        results.push(Value::Boolean(bloom.contains(item)));
                    }
                }
                _ => {
                    results.resize(self.items.len(), Value::Boolean(false));
                }
            },
        }
        Value::Array(Some(results))
    }
}

impl ReadRaftCommand for BfMExistsCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BfMExists(BfMExistsParams::parse(items)?))
    }
}

#[async_trait]
impl Command for BfMExistsCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let operation = self.read_operation(items)?;
        server.app.read(operation, client.db_number).await
    }
}
