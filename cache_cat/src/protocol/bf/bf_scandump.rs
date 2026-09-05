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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BfScanDumpParams {
    pub key: Bytes,
    pub iterator: i64,
}

impl Display for BfScanDumpParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BF.SCANDUMP {} {}",
            String::from_utf8_lossy(&self.key),
            self.iterator
        )
    }
}

impl BfScanDumpParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("BF.SCANDUMP"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let iterator = items[2]
            .parse_i64()
            .ok_or(ProtocolError::BloomScanDumpIteratorNotNumeric)?;

        Ok(Self { key, iterator })
    }
}

pub struct BfScanDumpCommand;

impl ReadCommand for BfScanDumpParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let entry = match value {
            Some(entry) => entry,
            None => return ProtocolError::BloomNotFound.into(),
        };

        let bloom = match &entry.value.data {
            ValueObject::Bloom(bloom) => bloom,
            _ => return ProtocolError::WrongType.into(),
        };

        let bloom = bloom.lock();
        let (iterator, data) = bloom.scan_dump(self.iterator);

        Value::Array(Some(vec![
            Value::Integer(iterator),
            Value::BulkString(Some(data)),
        ]))
    }
}

impl ReadRaftCommand for BfScanDumpCommand {
    fn read_operation(
        &self,
        items: &[Value],
    ) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BfScanDump(
            BfScanDumpParams::parse(items)?
        ))
    }
}

#[async_trait]
impl Command for BfScanDumpCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::SimpleString("QUEUED".to_string()));
        }

        let operation = self.read_operation(items)?;
        server.app.read(operation, client.db_number).await
    }
}