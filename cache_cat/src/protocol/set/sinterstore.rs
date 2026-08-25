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

/// Parameters for SINTERSTORE command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SInterStoreParams {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}



/// SINTERSTORE command executor
pub struct SInterStoreCommand;

impl SInterStoreCommand {
    fn parse(items: &[Value]) -> Result<SInterStoreParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("SINTERSTORE"));
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

        Ok(SInterStoreParams { key, keys })
    }
}

impl RaftCommand for SInterStoreCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Base(BaseOperation::SInterStore(
            SInterStoreReq {
                key: params.key,
                keys: params.keys,
            },
        )))
    }
}

#[async_trait]
impl Command for SInterStoreCommand {
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
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SInterStoreReq {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

impl Display for SInterStoreReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SINTERSTORE")?;
        write!(f, " {}", String::from_utf8_lossy(&self.key))?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl MultiReadComputeCommand for SInterStoreReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SInterStore(self)
    }

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let mut intersection: Option<HashSet<Bytes>> = None;

        for entry in read_entries {
            let Some(snapshot) = entry else {
                // Redis: 不存在的 key 视为空 Set。
                // intersection 必然为空。
                intersection = Some(HashSet::new());
                break;
            };

            let value = &snapshot.value;

            let ValueObject::Set(set) = &value.data else {
                return (
                    MochaOperation::Abort,
                    CacheCatError::from(ProtocolError::WrongType).into(),
                );
            };

            let set = set.lock();

            match intersection.as_mut() {
                None => {
                    // 第一组作为初始结果。
                    intersection = Some(set.iter().cloned().collect());
                }

                Some(result) => {
                    // retain 比 repeated intersection().collect() 少一些临时分配。
                    result.retain(|member| set.contains(member));

                    // 已经为空，后面不用再计算交集。

                    // 这里不能 break，见下面说明。
                    if result.is_empty() {
                        // 不 break，继续遍历剩余 entries 做 WRONGTYPE 检查。
                    }
                }
            }
        }

        let result = intersection.unwrap_or_default();
        let cardinality = result.len() as i64;
        if result.is_empty() {
            // Redis 不保存 empty set；结果为空相当于删除 destination。
            return (MochaOperation::Remove, Value::Integer(0));
        }
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
