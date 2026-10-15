//! LSET command implementation
//!
//! LSET key index value
//! Sets the list element at index to value.
//! Index is zero-based, with negative indices indicating elements starting at the end of the list.
//!
//! Returns:
//! - Simple string "OK" on success
//!
//! Errors:
//! - If the key does not exist, returns an error
//! - If the key exists but is not a list, returns an error
//! - If the index is out of range, returns an error

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::base_operation::BaseOperation::LSet;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// LSET command handler
pub struct LSetCommand;

impl LSetCommand {
    /// Parse arguments from RESP items
    /// Format: LSET key index value
    fn parse_args(items: &[Value]) -> Result<LSetArgs, ProtocolError> {
        // LSET key index value (4 items including command name)
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("lset"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        // Redis checks key existence/type before parsing LSET's index. Keep
        // valid indexes parsed up front for the common path, and defer only
        // the invalid result until ComputeCommand sees the current value.
        let index = items[2].parse_canonical_i64();
        let value = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        Ok(LSetArgs { key, index, value })
    }
}

/// Parsed LSET arguments
struct LSetArgs {
    key: Bytes,
    index: Option<i64>,
    value: Bytes,
}

impl RaftCommand for LSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(LSet(LSetReq {
            key: params.key,
            index: params.index,
            value: params.value,
        })))
    }
}

#[async_trait]
impl Command for LSetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LSetReq {
    pub key: Bytes,
    /// None defers an integer error until after the key/type check.
    pub index: Option<i64>,
    pub value: Bytes,
}

impl Display for LSetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LSetReq {{ key: {}, index: {:?}, value: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.index,
            self.value
        )
    }
}

impl ComputeCommand for LSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LSet(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let Some(index) = self.index else {
                    return (MochaOperation::Abort, ProtocolError::NotAnInteger.into());
                };
                let mut list = data_arc.lock();
                let len = list.len() as i64;

                // 处理负索引
                let index = if index < 0 { len + index } else { index };
                // 检查索引是否有效
                if index < 0 || index >= len {
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response("ERR index out of range").into(),
                    );
                }
                // 将索引转换为usize并替换元素
                let idx = index as usize;
                if let Some(element) = list.get_mut(idx) {
                    *element = self.value;
                } else {
                    // 理论上不会发生，因为我们已经检查了索引范围
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response("ERR index out of range").into(),
                    );
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::SimpleString("OK".to_string()),
                )
            }
            _ => (MochaOperation::Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // LSET cannot create a new key
        (
            MochaOperation::Abort,
            ProtocolError::response("ERR no such key").into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::ExpirePolicy;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn request(index: &str) -> LSetReq {
        let items = ["LSET", "list", index, "value"]
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
        let operation = LSetCommand.raft_request(&items).unwrap();
        let encoded = bincode2::serialize(&operation).unwrap();
        let Operation::Base(BaseOperation::LSet(params)) = bincode2::deserialize(&encoded).unwrap()
        else {
            panic!("expected LSET");
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
    fn index_uses_redis_integer_syntax() {
        for index in [
            "",
            "+0",
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
            let (operation, response) = request(index).mutate(list_entry(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(
                response.encode(),
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
            assert_eq!(params.index, Some(index.parse::<i64>().unwrap()));
        }
    }

    #[test]
    fn invalid_index_is_checked_after_missing_key_and_type() {
        let params = request("+0");

        let (operation, response) = params.clone().init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            response,
            Value::Error(error) if error == "ERR no such key"
        ));

        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let (operation, response) = params.clone().mutate(wrong_type, 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            response,
            Value::Error(error) if error == ProtocolError::WrongType.to_string()
        ));

        let list = list_entry();
        let (operation, response) = params.mutate(list.clone(), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert!(matches!(
            response,
            Value::Error(error) if error == ProtocolError::NotAnInteger.to_string()
        ));
        let ValueObject::List(contents) = &list.value.data else {
            panic!("expected list");
        };
        assert_eq!(contents.lock().front().unwrap(), b"a".as_slice());
        assert_eq!(list.expire_at, Some(100));
    }

    #[test]
    fn canonical_index_preserves_ttl_and_range_errors_do_not_change_the_list() {
        let list = list_entry();
        for index in ["2", "-3", "-9223372036854775808", "9223372036854775807"] {
            let (operation, response) = request(index).mutate(list.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(response.encode(), b"-ERR index out of range\r\n");
        }
        let (operation, response) = request("-1").mutate(list.clone(), 0);
        assert_eq!(response.encode(), b"+OK\r\n");
        assert!(matches!(
            operation,
            MochaOperation::Insert {
                expire: ExpirePolicy::Absolute(100),
                ..
            }
        ));
        let ValueObject::List(contents) = &list.value.data else {
            panic!("expected list");
        };
        assert_eq!(
            *contents.lock(),
            VecDeque::from([Bytes::from_static(b"a"), Bytes::from_static(b"value")]),
        );
    }
}
