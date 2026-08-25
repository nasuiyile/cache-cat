use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::MultiReadComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

/// Parameters for SUNIONSTORE command
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SUnionStoreParams {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

/// SUNIONSTORE command executor
pub struct SUnionStoreCommand;

impl SUnionStoreCommand {
    fn parse(items: &[Value]) -> Result<SUnionStoreParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("SUNIONSTORE"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let keys = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        Ok(SUnionStoreParams { key, keys })
    }
}

impl RaftCommand for SUnionStoreCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Base(BaseOperation::SUnionStore(
            SUnionStoreReq {
                key: params.key,
                keys: params.keys,
            },
        )))
    }
}

#[async_trait]
impl Command for SUnionStoreCommand {
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
        let operation = self.raft_request(items)?;
        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SUnionStoreReq {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

impl Display for SUnionStoreReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SUNIONSTORE")?;
        write!(f, " {}", String::from_utf8_lossy(&self.key))?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl MultiReadComputeCommand for SUnionStoreReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SUnionStore(self)
    }

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let mut result: HashSet<Bytes> = Default::default();

        for entry in read_entries {
            let Some(snapshot) = entry else {
                continue;
            };

            let ValueObject::Set(data) = &snapshot.value.data else {
                return (
                    MochaOperation::Abort,
                    CacheCatError::from(ProtocolError::WrongType).into(),
                );
            };

            result.extend(data.lock().iter().cloned());
        }

        if result.is_empty() {
            (MochaOperation::Remove, Value::Integer(0))
        } else {
            let cardinality = result.len() as i64;
            let value = MyValue::new(ValueObject::Set(Arc::new(Mutex::new(result))));

            (
                MochaOperation::Insert {
                    value,
                    expire: ExpirePolicy::Persistent,
                },
                Value::Integer(cardinality),
            )
        }
    }
}
