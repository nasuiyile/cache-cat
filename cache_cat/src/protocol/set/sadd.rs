use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::SAdd;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

struct SAddArgs {
    key: Bytes,
    members: Vec<Bytes>,
}

pub struct SAddCommand;

impl SAddCommand {
    fn parse_args(items: &[Value]) -> Result<SAddArgs, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("sadd"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let members = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if members.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("member"));
        }

        Ok(SAddArgs { key, members })
    }
}

impl RaftCommand for SAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(SAdd(SAddReq {
            key: params.key,
            elements: params.members,
        })))
    }
}

#[async_trait]
impl Command for SAddCommand {
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
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SAddReq {
    pub key: Bytes,
    pub elements: Vec<Bytes>,
}

impl fmt::Display for SAddReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SAddReq {{ key: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

impl ComputeCommand for SAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SAdd(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Set(set) => {
                let mut count = 0;
                {
                    let mut set = set.lock();
                    for v in &self.elements {
                        if set.insert(v.clone()) {
                            count += 1;
                        }
                    }
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(count),
                )
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut set = HashSet::new();
        let len = self.elements.len();
        for v in self.elements {
            set.insert(v);
        }
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len as i64),
        )
    }
}
