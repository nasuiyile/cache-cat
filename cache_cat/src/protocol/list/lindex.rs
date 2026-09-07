use crate::error::{CacheCatError, ProtocolError};
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
use crate::mocha::EntrySnapshot;

pub struct LIndexCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LIndexParams {
    pub key: Bytes,
    pub index: i64,
}

impl Display for LIndexParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LIndexParams {{ key: {}, index: {} }}",
            String::from_utf8_lossy(&self.key),
            self.index
        )
    }
}

impl ReadCommand for LIndexParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.value.data {
                ValueObject::List(list) => {
                    let deque = list.lock();
                    let len = deque.len() as i64;

                    // 处理负数索引，转换为正数索引
                    let idx = if self.index < 0 {
                        len + self.index
                    } else {
                        self.index
                    };

                    // 检查索引是否在有效范围内
                    if idx < 0 || idx >= len {
                        return Value::BulkString(None);
                    }

                    // 获取指定位置的元素
                    if let Some(element) = deque.get(idx as usize) {
                        Value::BulkString(Some(element.clone()))
                    } else {
                        Value::BulkString(None)
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl LIndexCommand {
    fn parse_args(items: &[Value]) -> Result<LIndexParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("lindex"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let index = items[2].try_parse_i64()?;

        Ok(LIndexParams { key, index })
    }
}

impl ReadRaftCommand for LIndexCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::LIndex(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for LIndexCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // 如果在事务中，将命令加入队列
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        // 正常执行读取操作
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}