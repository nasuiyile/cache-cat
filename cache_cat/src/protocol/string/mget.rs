use crate::error::{CacheCatError, ProtocolError};
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
use std::fmt::Display;
use crate::mocha::EntrySnapshot;

/// Parameters for MGET command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MgetParams {
    pub keys: Vec<Bytes>,
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

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 1 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        Ok(MgetParams { keys })
    }
}

/// MGET command executor
pub struct MgetCommand;

impl ReadRaftCommand for MgetCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::MGet(MgetParams::parse(items)?))
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
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}

impl MultiReadCommand for MgetParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        let mut results = Vec::with_capacity(values.len());
        
        for value in values {
            results.push(match value {
                None => Value::BulkString(None),

                Some(v) => match v.value.data {
                    ValueObject::Int(int_value) => {
                        Value::BulkString(Some(int_value.to_string().into()))
                    }

                    ValueObject::String(str_value) => Value::BulkString(Some(str_value)),

                    _ => ProtocolError::WrongType.into(),
                },
            });
        }

        Value::Array(Some(results))
    }
}
