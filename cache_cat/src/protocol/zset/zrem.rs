use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::ZRem;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZRemParam {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl fmt::Display for ZRemParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZRemParam {{ key: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.members
        )
    }
}

pub struct ZRemCommand;

impl ZRemCommand {
    fn parse_params(items: &[Value]) -> Result<ZRemParam, ProtocolError> {
        // Minimum: ZREM key member [member ...]
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("zrem"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let mut members = Vec::with_capacity(items.len() - 2);
        for item in &items[2..] {
            let member = item
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("member"))?;
            members.push(member);
        }

        Ok(ZRemParam { key, members })
    }
}

impl RaftCommand for ZRemCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_params(items)?;

        Ok(Operation::Base(ZRem(ZRemReq {
            key: params.key,
            members: params.members,
        })))
    }
}

#[async_trait]
impl Command for ZRemCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZRemReq {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl fmt::Display for ZRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZRemReq {{ key: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.members
        )
    }
}

impl ComputeCommand for ZRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        ZRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ZSet(zset) => {
                let removed_count = zset.lock().zrem(&self.members);

                // 如果删除后集合为空，则删除整个 key
                if zset.lock().is_empty() {
                    (MochaOperation::Remove, Value::Integer(removed_count))
                } else {
                    (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::Integer(removed_count),
                    )
                }
            }
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // 如果 key 不存在，直接返回 0
        (MochaOperation::Abort, Value::Integer(0))
    }
}
