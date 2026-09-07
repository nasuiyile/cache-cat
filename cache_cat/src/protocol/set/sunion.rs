use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Display;

/// Parameters for SUNION command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SUnionParams {
    pub keys: Vec<Bytes>,
}

impl Display for SUnionParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUNION")?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl SUnionParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("SUNION"));
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 1 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        Ok(SUnionParams { keys })
    }
}

/// SUNION command executor
pub struct SUnionCommand;

impl ReadRaftCommand for SUnionCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SUnion(SUnionParams::parse(items)?))
    }
}

#[async_trait]
impl Command for SUnionCommand {
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
        server.app.multi_read(params, client.db_number).await
    }
}

impl MultiReadCommand for SUnionParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        // use `HashSet` for quickly insert
        let mut results: HashSet<_> = Default::default();

        for value in values {
            let Some(value) = value else {
                continue;
            };

            let ValueObject::Set(data) = value.value.data else {
                return ProtocolError::InvalidArgument("There is a value that is not a set").into();
            };

            results.extend(data.lock().iter().cloned());
        }

        let results = results
            .into_iter()
            .map(|v| Value::BulkString(Some(v)))
            .collect();

        // Set reply (RESP2 *N, RESP3 ~N).
        Value::Set(results)
    }
}
