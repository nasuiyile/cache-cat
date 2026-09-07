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

pub struct SCardCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SCardParams {
    pub key: Bytes,
}

impl Display for SCardParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SCardParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ReadCommand for SCardParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            // 对应 Redis: key 不存在时返回 0
            None => Value::Integer(0),
            Some(v) => match v.value.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();
                    // 对应 Redis SCARD: 返回集合元素个数
                    Value::Integer(guard.len() as i64)
                }
                // 对应 Redis: key 存在但不是集合类型时返回错误
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl SCardCommand {
    fn parse_args(items: &[Value]) -> Result<SCardParams, ProtocolError> {
        // SCARD 只接受 1 个参数: SCARD key
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("scard"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(SCardParams { key })
    }
}

impl ReadRaftCommand for SCardCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SCard(SCardCommand::parse_args(items)?))
    }
}

#[async_trait]
impl Command for SCardCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // 如果在事务上下文中，将命令加入队列
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}