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

pub struct LIndexCommand;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::entry::request::Operation;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn request(index: &str) -> LIndexParams {
        let items = ["LINDEX", "list", index]
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
        let operation = LIndexCommand.raft_request(&items).unwrap();
        let encoded = bincode2::serialize(&operation).unwrap();
        let Operation::Read(ReadOperation::LIndex(params)) =
            bincode2::deserialize(&encoded).unwrap()
        else {
            panic!("expected LINDEX");
        };
        params
    }

    fn list_entry() -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(VecDeque::from([
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]))))),
            expire_at: Some(100),
        }
    }

    #[test]
    fn index_requires_a_canonical_integer() {
        for index in [
            "",
            "+1",
            "00",
            "01",
            "-0",
            "-01",
            " 1",
            "1 ",
            "1\0",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            assert_eq!(
                request(index).execute(Some(list_entry())).encode(),
                b"-ERR value is not an integer or out of range\r\n",
                "index {index:?}",
            );
        }
        for index in [
            "0",
            "1",
            "-1",
            "-9223372036854775808",
            "9223372036854775807",
        ] {
            let params = request(index);
            assert_eq!(params.index, index.parse::<i64>().unwrap());
            assert!(params.index_valid);
        }
        for (index, expected) in [
            ("0", b"$1\r\na\r\n".as_slice()),
            ("-1", b"$1\r\nb\r\n".as_slice()),
            ("-9223372036854775808", b"$-1\r\n".as_slice()),
            ("9223372036854775807", b"$-1\r\n".as_slice()),
        ] {
            assert_eq!(
                request(index).execute(Some(list_entry())).encode(),
                expected
            );
        }
    }

    #[test]
    fn invalid_index_is_checked_after_missing_key_and_type() {
        let params = request("+0");

        assert!(matches!(params.execute(None), Value::BulkString(None)));

        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        assert!(matches!(
            params.execute(Some(wrong_type)),
            Value::Error(error) if error == ProtocolError::WrongType.to_string()
        ));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LIndexParams {
    pub key: Bytes,
    pub index: i64,
    /// Delay an invalid-index reply until after the key's existence/type check.
    pub index_valid: bool,
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
                    if !self.index_valid {
                        return ProtocolError::NotAnInteger.into();
                    }
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

        // Parse once without retaining argument bytes. The error is reported
        // only after the key lookup and WRONGTYPE check, as Redis requires.
        let parsed_index = items[2].parse_canonical_i64();
        let index_valid = parsed_index.is_some();
        let index = parsed_index.unwrap_or_default();

        Ok(LIndexParams {
            key,
            index,
            index_valid,
        })
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
