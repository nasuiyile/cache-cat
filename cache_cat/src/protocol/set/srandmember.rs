use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
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
use rand::seq::IteratorRandom;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub struct SRandMemberCommand;

/// SRANDMEMBER 的 count 参数是否存在，会影响返回类型：
///
/// SRANDMEMBER key
///     -> BulkString
///
/// SRANDMEMBER key count
///     -> Array
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRandMemberParams {
    pub key: Bytes,
    pub count: Option<i64>,
}

impl Display for SRandMemberParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.count {
            Some(count) => write!(
                f,
                "SRandMemberParams {{ key: {}, count: {} }}",
                String::from_utf8_lossy(&self.key),
                count
            ),
            None => write!(
                f,
                "SRandMemberParams {{ key: {}, count: None }}",
                String::from_utf8_lossy(&self.key)
            ),
        }
    }
}

impl ReadCommand for SRandMemberParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match value {
            None => self.empty_result(),

            Some(snapshot) => match snapshot.value.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();

                    if guard.is_empty() {
                        return self.empty_result();
                    }

                    match self.count {
                        // SRANDMEMBER key
                        //
                        // 不带 count 时返回单个 BulkString；
                        // 集合为空时返回 Nil BulkString。
                        None => {
                            let mut rng = rand::thread_rng();

                            match guard.iter().choose(&mut rng) {
                                Some(member) => {
                                    Value::BulkString(Some(member.clone()))
                                }
                                None => Value::BulkString(None),
                            }
                        }

                        // SRANDMEMBER key 0
                        Some(0) => Value::Array(Some(Vec::new())),

                        // SRANDMEMBER key positive-count
                        //
                        // 返回不重复的随机元素。
                        // 如果 count 大于集合大小，只返回集合中的全部元素。
                        Some(count) if count > 0 => {
                            let requested = match usize::try_from(count) {
                                Ok(count) => count,
                                Err(_) => {
                                    return Value::Array(Some(Vec::new()));
                                }
                            };

                            let take_count = requested.min(guard.len());
                            let mut rng = rand::thread_rng();

                            let members = guard
                                .iter()
                                .choose_multiple(&mut rng, take_count)
                                .into_iter()
                                .map(|member| {
                                    Value::BulkString(Some(member.clone()))
                                })
                                .collect();

                            Value::Array(Some(members))
                        }

                        // SRANDMEMBER key negative-count
                        //
                        // 允许返回重复元素，并且必须返回 abs(count) 个元素。
                        Some(count) => {
                            let requested = match count
                                .checked_abs()
                                .and_then(|count| usize::try_from(count).ok())
                            {
                                Some(count) => count,

                                // i64::MIN 无法使用有符号 i64 表示其绝对值。
                                // 正常情况下也不应允许客户端要求如此巨大的响应。
                                None => {
                                    return ProtocolError::response(
                                        "ERR value is out of range, must be positive",
                                    )
                                    .into();
                                }
                            };

                            let members: Vec<&Bytes> = guard.iter().collect();
                            let mut rng = rand::thread_rng();

                            let mut result = Vec::new();

                            // 避免一次 reserve 巨大内存时直接 panic。
                            if result.try_reserve(requested).is_err() {
                                return ProtocolError::response("ERR count is too large").into();
                            }

                            for _ in 0..requested {
                                let index = rng.gen_range(0..members.len());

                                result.push(Value::BulkString(Some(
                                    members[index].clone(),
                                )));
                            }

                            Value::Array(Some(result))
                        }
                    }
                }

                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl SRandMemberParams {
    /// key 不存在或集合为空时：
    ///
    /// 不带 count：
    ///     返回 Nil BulkString。
    ///
    /// 带 count：
    ///     返回空数组。
    fn empty_result(&self) -> Value {
        match self.count {
            None => Value::BulkString(None),
            Some(_) => Value::Array(Some(Vec::new())),
        }
    }
}

impl SRandMemberCommand {
    fn parse_args(items: &[Value]) -> Result<SRandMemberParams, ProtocolError> {
        if items.len() != 2 && items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("srandmember"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let count = if items.len() == 3 {
            let count_bytes = items[2]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("count"))?;

            let count_str = std::str::from_utf8(count_bytes.as_ref())
                .map_err(|_| ProtocolError::InvalidArgument("count"))?;

            let count = count_str
                .parse::<i64>()
                .map_err(|_| ProtocolError::InvalidArgument("count"))?;

            Some(count)
        } else {
            None
        };

        Ok(SRandMemberParams { key, count })
    }
}

impl ReadRaftCommand for SRandMemberCommand {
    fn read_operation(
        &self,
        items: &[Value],
    ) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SRandMember(
            SRandMemberCommand::parse_args(items)?,
        ))
    }
}

#[async_trait]
impl Command for SRandMemberCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);

            return Ok(Value::queued());
        }

        let operation = self.read_operation(items)?;

        server.app.read(operation, client.db_number).await
    }
}
