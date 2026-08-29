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
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for BF.EXISTS command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BfExistsParams {
    pub key: Bytes,
    pub item: Bytes,
}

impl Display for BfExistsParams {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "BF.EXISTS {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.item),
        )
    }
}

impl BfExistsParams {
    fn parse(
        items: &[Value],
    ) -> Result<Self, ProtocolError> {
        // BF.EXISTS key item
        if items.len() != 3 {
            return Err(
                ProtocolError::WrongArgCount(
                    "BF.EXISTS"
                )
            );
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument(
                    "key"
                )
            )?;

        let item = items[2]
            .string_bytes_clone()
            .ok_or(
                ProtocolError::InvalidArgument(
                    "item"
                )
            )?;

        Ok(Self {
            key,
            item,
        })
    }
}

/// BF.EXISTS command executor
pub struct BfExistsCommand;

impl ReadCommand for BfExistsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(
        &self,
        value: Option<EntrySnapshot<MyValue>>,
    ) -> Value {
        let exists = match value {
            /*
             * Redis:
             *
             * BF.EXISTS nonexistent item
             *
             * -> RESP2 :0
             * -> RESP3 #f
             */
            None => false,

            Some(entry) => {
                match &entry.value.data {
                    ValueObject::Bloom(bloom) => {
                        /*
                         * BloomObject 被 Arc<Mutex<_>>
                         * 共享。
                         *
                         * parking_lot::Mutex::lock()
                         * 不需要 unwrap。
                         */
                        let bloom = bloom.lock();

                        bloom.contains(
                            &self.item
                        )
                    }

                    /*
                     * 这里非常容易误写成 WRONGTYPE。
                     *
                     * RedisBloom BF.EXISTS 的当前行为是：
                     *
                     * key 是其他类型 -> false
                     */
                    _ => false,
                }
            }
        };

        Value::Boolean(exists)
    }
}

impl ReadRaftCommand for BfExistsCommand {
    fn read_operation(
        &self,
        items: &[Value],
    ) -> Result<ReadOperation, ProtocolError> {
        Ok(
            ReadOperation::BfExists(
                BfExistsParams::parse(items)?
            )
        )
    }
}

#[async_trait]
impl Command for BfExistsCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        /*
         * MULTI / EXEC。
         *
         * 和 STRLEN 保持完全一致。
         */
        if let Some(queue) =
            client.transaction_queue.as_mut()
        {
            queue.push(
                self.raft_request(items)?
            );

            return Ok(
                Value::SimpleString(
                    "QUEUED".to_string()
                )
            );
        }

        let operation =
            self.read_operation(items)?;

        server
            .app
            .read(
                operation,
                client.db_number,
            )
            .await
    }
}