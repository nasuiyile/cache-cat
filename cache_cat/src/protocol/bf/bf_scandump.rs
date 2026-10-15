use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::bf::error::NOT_FOUND;
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
            return Err(ProtocolError::WrongArgCount("bf.scandump"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let iterator = items[2]
            .try_parse_canonical_i64()
            .map_err(|_| ProtocolError::response("Second argument must be numeric"))?;
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
            None => return ProtocolError::response(NOT_FOUND).into(),
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
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BfScanDump(BfScanDumpParams::parse(items)?))
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
            return Ok(Value::queued());
        }
        let operation = self.read_operation(items)?;
        server.app.read(operation, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_follows_module_canonical_integer_rules() {
        let args = |number: &str| {
            ["BF.SCANDUMP", "key", number]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
        };
        for number in [
            "",
            "+1",
            "01",
            "-0",
            "-01",
            " 1",
            "1 ",
            "1\0",
            "9223372036854775808",
        ] {
            assert_eq!(
                BfScanDumpParams::parse(&args(number))
                    .unwrap_err()
                    .to_string(),
                "Second argument must be numeric"
            );
        }
        for number in [
            "0",
            "1",
            "-1",
            "-9223372036854775808",
            "9223372036854775807",
        ] {
            assert_eq!(
                BfScanDumpParams::parse(&args(number)).unwrap().iterator,
                number.parse::<i64>().unwrap()
            );
        }
    }
}
