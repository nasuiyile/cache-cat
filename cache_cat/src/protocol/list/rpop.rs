//! RPOP command implementation
//!
//! RPOP key [count]
//! Remove and return the last element of the list stored at key.
//!
//! Returns:
//! - The last element of the list
//! - Nil if key does not exist
//! - Array of elements when count is specified

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::MochaOperation::Abort;
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::base_operation::BaseOperation::RPop;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// RPOP command handler
pub struct RPopCommand;

impl RPopCommand {
    /// Parse arguments
    /// Format: RPOP key [count]
    fn parse_args(items: &[Value]) -> Result<RPopArgs, ProtocolError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(ProtocolError::WrongArgCount("rpop"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let count = if items.len() == 3 {
            let count = items[2].parse_i64().ok_or(ProtocolError::response(
                "ERR value is out of range, must be positive",
            ))?;
            if count < 0 {
                return Err(ProtocolError::response(
                    "ERR value is out of range, must be positive",
                ));
            }
            Some(count as u64)
        } else {
            None
        };
        Ok(RPopArgs { key, count })
    }
}

/// Parsed RPOP arguments
struct RPopArgs {
    key: Bytes,
    count: Option<u64>,
}

impl RaftCommand for RPopCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(RPop(RPopReq {
            key: params.key,
            count: params.count,
        })))
    }
}

#[async_trait]
impl Command for RPopCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RPopReq {
    pub key: Bytes,
    pub count: Option<u64>,
}

impl Display for RPopReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RPopReq {{ key: {}, count: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.count
        )
    }
}

impl ComputeCommand for RPopReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::RPop(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let mut list = data_arc.lock();
                if self.count == Some(0) {
                    return (Abort, Value::Array(Some(Vec::new())));
                }
                let response = match self.count {
                    None => Value::BulkString(list.pop_back()),
                    Some(count) => {
                        let count = count.min(list.len() as u64) as usize;
                        let start = list.len() - count;
                        let popped = list
                            .drain(start..)
                            .rev()
                            .map(|value| Value::BulkString(Some(value)))
                            .collect();
                        Value::Array(Some(popped))
                    }
                };
                if list.is_empty() {
                    (MochaOperation::Remove, response)
                } else {
                    (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        response,
                    )
                }
            }
            _ => (Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let response = match self.count {
            None => Value::BulkString(None),
            Some(_) => Value::Array(None),
        };
        (Abort, response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::ExpirePolicy;
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn entry() -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(VecDeque::from([
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]))))),
            expire_at: Some(100),
        }
    }

    fn request(count: Option<&str>) -> RPopReq {
        let mut items = vec![
            Value::BulkString(Some(Bytes::from_static(b"RPOP"))),
            Value::BulkString(Some(Bytes::from_static(b"list"))),
        ];
        if let Some(count) = count {
            items.push(Value::BulkString(Some(Bytes::copy_from_slice(
                count.as_bytes(),
            ))));
        }
        let Operation::Base(BaseOperation::RPop(request)) =
            RPopCommand.raft_request(&items).unwrap()
        else {
            panic!("expected RPOP");
        };
        request
    }

    #[test]
    fn count_pops_from_tail_and_removes_key_after_last_element() {
        let snapshot = entry();
        let (operation, response) = request(Some("2")).mutate(snapshot.clone(), 0);
        assert_eq!(response.encode(), b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n");
        assert!(matches!(
            operation,
            MochaOperation::Insert {
                expire: ExpirePolicy::Absolute(100),
                ..
            }
        ));
        let (operation, response) = request(None).mutate(snapshot, 0);
        assert_eq!(response.encode(), b"$1\r\na\r\n");
        assert!(matches!(operation, MochaOperation::Remove));
        assert_eq!(
            request(Some("1")).mutate(entry(), 0).1.encode(),
            b"*1\r\n$1\r\nc\r\n"
        );
    }

    #[test]
    fn zero_count_is_noop_and_missing_count_form_is_null_array() {
        let snapshot = entry();
        let (operation, response) = request(Some("0")).mutate(snapshot.clone(), 0);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(response.encode(), b"*0\r\n");
        assert_eq!(request(None).mutate(snapshot, 0).1.encode(), b"$1\r\nc\r\n");
        assert_eq!(request(None).init().1.encode(), b"$-1\r\n");
        assert_eq!(request(Some("0")).init().1.encode(), b"*-1\r\n");
        assert!(matches!(
            request(Some("10")).mutate(entry(), 0).0,
            MochaOperation::Remove
        ));
    }

    #[test]
    fn rejects_negative_and_overflowing_counts() {
        for count in ["-1", "9223372036854775808", "invalid"] {
            let items = ["RPOP", "list", count]
                .map(|s| Value::BulkString(Some(Bytes::copy_from_slice(s.as_bytes()))));
            assert_eq!(
                RPopCommand.raft_request(&items).unwrap_err().to_string(),
                "ERR value is out of range, must be positive"
            );
        }
    }
}
