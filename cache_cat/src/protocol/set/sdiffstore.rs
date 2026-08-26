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

/// Parameters for SDIFFSTORE command
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SDiffStoreParams {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

/// SDIFFSTORE command executor
pub struct SDiffStoreCommand;

impl SDiffStoreCommand {
    fn parse(items: &[Value]) -> Result<SDiffStoreParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("SDIFFSTORE"));
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

        Ok(SDiffStoreParams { key, keys })
    }
}

impl RaftCommand for SDiffStoreCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Base(BaseOperation::SDiffStore(SDiffStoreReq {
            key: params.key,
            keys: params.keys,
        })))
    }
}

#[async_trait]
impl Command for SDiffStoreCommand {
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
pub struct SDiffStoreReq {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

impl Display for SDiffStoreReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SDIFFSTORE")?;
        write!(f, " {}", String::from_utf8_lossy(&self.key))?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl MultiReadComputeCommand for SDiffStoreReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SDiffStore(self)
    }

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let mut entries = read_entries.into_iter();

        let mut diffsection: Option<HashSet<Bytes>> = if let Some(entry) = entries.next() {
            if let Some(snapshot) = entry {
                let ValueObject::Set(data) = &snapshot.value.data else {
                    return (
                        MochaOperation::Abort,
                        CacheCatError::from(ProtocolError::WrongType).into(),
                    );
                };

                Some(data.lock().iter().cloned().collect())
            } else {
                None
            }
        } else {
            return (
                MochaOperation::Abort,
                CacheCatError::from(ProtocolError::WrongArgCount(
                    "wrong number of arguments for command",
                ))
                .into(),
            );
        };

        for entry in entries {
            let Some(snapshot) = entry else {
                continue;
            };

            let ValueObject::Set(data) = &snapshot.value.data else {
                return (
                    MochaOperation::Abort,
                    CacheCatError::from(ProtocolError::WrongType).into(),
                );
            };

            if let Some(ref mut result) = diffsection
                && !result.is_empty()
            {
                let data = data.lock();
                result.retain(|v| !data.contains(v));
            }
        }

        if let Some(result) = diffsection
            && !result.is_empty()
        {
            let cardinality = result.len() as i64;
            let value = MyValue::new(ValueObject::Set(Arc::new(Mutex::new(result))));

            (
                MochaOperation::Insert {
                    value,
                    expire: ExpirePolicy::Persistent,
                },
                Value::Integer(cardinality),
            )
        } else {
            (MochaOperation::Remove, Value::Integer(0))
        }
    }
}
