use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::sorted_set::SortedSet;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::ZIncrBy;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZIncrByParam {
    pub key: Bytes,
    pub increment: f64,
    pub member: Bytes,
}

impl fmt::Display for ZIncrByParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZIncrByParam {{ key: {}, increment: {}, member: {} }}",
            String::from_utf8_lossy(&self.key),
            self.increment,
            String::from_utf8_lossy(&self.member),
        )
    }
}

pub struct ZIncrByCommand;

impl ZIncrByCommand {
    fn parse_params(items: &[Value]) -> Result<ZIncrByParam, ProtocolError> {
        // ZINCRBY key increment member
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("zincrby"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let increment_bytes = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("increment"))?;

        let increment_str = std::str::from_utf8(&increment_bytes)
            .map_err(|_| ProtocolError::InvalidArgument("increment"))?;

        let increment = increment_str
            .parse::<f64>()
            .map_err(|_| ProtocolError::InvalidArgument("increment"))?;

        // Redis does not accept NaN as increment.
        if increment.is_nan() {
            return Err(ProtocolError::InvalidArgument("increment"));
        }

        let member = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("member"))?;

        Ok(ZIncrByParam {
            key,
            increment,
            member,
        })
    }
}

impl RaftCommand for ZIncrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_params(items)?;

        Ok(Operation::Base(ZIncrBy(ZIncrByReq {
            key: params.key,
            increment: params.increment,
            member: params.member,
        })))
    }
}

#[async_trait]
impl Command for ZIncrByCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Keep behavior consistent with INCRBY.
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZIncrByReq {
    pub key: Bytes,
    pub increment: f64,
    pub member: Bytes,
}

impl fmt::Display for ZIncrByReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZIncrByReq {{ key: {}, increment: {}, member: {} }}",
            String::from_utf8_lossy(&self.key),
            self.increment,
            String::from_utf8_lossy(&self.member),
        )
    }
}

impl ComputeCommand for ZIncrByReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        ZIncrBy(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::ZSet(zset) => {
                let new_score = {
                    let mut zset = zset.lock();

                    match zset.zincrby(self.member, self.increment) {
                        Some(score) => score,
                        None => {
                            return (
                                MochaOperation::Abort,
                                ProtocolError::response(
                                    "ERR resulting score is not a number (NaN)",
                                )
                                .into(),
                            );
                        }
                    }
                };

                (
                    MochaOperation::Insert {
                        // The Arc<Mutex<SortedSet>> has already been mutated.
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    // Double reply: RESP2 bulk string, RESP3 double.
                    Value::Double(new_score),
                )
            }

            _ => (MochaOperation::Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut zset = SortedSet::new();

        let new_score = match zset.zincrby(self.member, self.increment) {
            Some(score) => score,
            None => {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response("ERR resulting score is not a number (NaN)").into(),
                );
            }
        };

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(zset)))),
                expire: ExpirePolicy::Persistent,
            },
            // Double reply: RESP2 bulk string, RESP3 double.
            Value::Double(new_score),
        )
    }
}
