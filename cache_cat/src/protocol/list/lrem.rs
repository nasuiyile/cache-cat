//! LREM command implementation
//!
//! LREM key count element
//! Remove the first count occurrences of element from the list stored at key.
//!
//! The count argument influences the operation:
//! - count > 0: Remove elements equal to element moving from head to tail
//! - count < 0: Remove elements equal to element moving from tail to head
//! - count = 0: Remove all elements equal to element
//!
//! Returns:
//! - The number of removed elements
//! - 0 if key does not exist
//! - Error if key exists but is not a list

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
use crate::raft::types::entry::base_operation::BaseOperation::LRem;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Display;

/// LREM command handler
pub struct LRemCommand;

impl LRemCommand {
    /// Parse arguments
    /// Format: LREM key count element
    fn parse_args(items: &[Value]) -> Result<LRemArgs, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("lrem"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let count = items[2]
            .parse_i64()
            .ok_or(ProtocolError::InvalidArgument("count"))?;
        let element = items[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("element"))?;
        Ok(LRemArgs {
            key,
            count,
            element,
        })
    }
}

/// Parsed LREM arguments
struct LRemArgs {
    key: Bytes,
    count: i64,
    element: Bytes,
}

impl RaftCommand for LRemCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(LRem(LRemReq {
            key: params.key,
            count: params.count,
            element: params.element,
        })))
    }
}

#[async_trait]
impl Command for LRemCommand {
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
pub struct LRemReq {
    pub key: Bytes,
    pub count: i64,
    pub element: Bytes,
}

impl Display for LRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LRemReq {{ key: {}, count: {}, element: {} }}",
            String::from_utf8_lossy(&self.key),
            self.count,
            String::from_utf8_lossy(&self.element)
        )
    }
}

impl ComputeCommand for LRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let mut list = data_arc.lock();
                let removed_count = self.remove_elements(&mut list);
                if list.is_empty() {
                    return (MochaOperation::Remove, Value::Integer(removed_count));
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(removed_count),
                )
            }
            _ => (Abort, ProtocolError::WrongType.into()),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // Key不存在时返回0
        (Abort, Value::Integer(0))
    }
}

impl LRemReq {
    /// Remove elements from the list based on count value
    fn remove_elements(&self, list: &mut VecDeque<Bytes>) -> i64 {
        match self.count.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let mut removed = 0;
                let mut end = 0;
                while end < list.len() && removed < self.count as u64 {
                    if list[end] == self.element {
                        removed += 1;
                    }
                    end += 1;
                }
                if removed == 0 {
                    return 0;
                }

                // Compact only the scanned prefix towards its end, then drop
                // the removed slots from the front. The suffix stays in place,
                // so removing a few elements near the head remains cheap.
                let mut write = end;
                for read in (0..end).rev() {
                    if list[read] != self.element {
                        write -= 1;
                        list.swap(read, write);
                    }
                }
                list.drain(..removed as usize);
                removed as i64
            }
            std::cmp::Ordering::Less => {
                let mut removed = 0;
                let mut start = list.len();
                while start > 0 && removed < self.count.unsigned_abs() {
                    start -= 1;
                    if list[start] == self.element {
                        removed += 1;
                    }
                }
                if removed == 0 {
                    return 0;
                }

                // Compact the scanned suffix towards its start. Each survivor
                // moves at most once, including when matches are interleaved.
                let mut write = start;
                for read in start..list.len() {
                    if list[read] != self.element {
                        list.swap(read, write);
                        write += 1;
                    }
                }
                list.truncate(list.len() - removed as usize);
                removed as i64
            }
            std::cmp::Ordering::Equal => {
                let old_len = list.len();
                list.retain(|value| value != &self.element);
                (old_len - list.len()) as i64
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;

    #[test]
    fn count_direction_preserves_survivor_order_in_wrapped_lists() {
        for (count, expected_removed, expected) in [
            (1, 1, vec!["b", "a", "c", "a", "d", "a"]),
            (2, 2, vec!["b", "c", "a", "d", "a"]),
            (-1, 1, vec!["a", "b", "a", "c", "a", "d"]),
            (-2, 2, vec!["a", "b", "a", "c", "d"]),
            (0, 4, vec!["b", "c", "d"]),
            (i64::MAX, 4, vec!["b", "c", "d"]),
            (i64::MIN, 4, vec!["b", "c", "d"]),
        ] {
            let mut list = VecDeque::with_capacity(7);
            // Fill and advance the ring before adding the remaining values.
            for value in ["padding", "padding", "padding", "padding", "a", "b", "a"] {
                list.push_back(Bytes::from_static(value.as_bytes()));
            }
            list.drain(..4);
            for value in ["c", "a", "d", "a"] {
                list.push_back(Bytes::from_static(value.as_bytes()));
            }
            assert!(!list.as_slices().1.is_empty());

            let removed = LRemReq {
                key: "list".into(),
                count,
                element: "a".into(),
            }
            .remove_elements(&mut list);
            assert_eq!(removed, expected_removed, "count {count}");
            assert_eq!(
                list,
                expected
                    .into_iter()
                    .map(Bytes::from)
                    .collect::<VecDeque<_>>(),
                "count {count}"
            );
        }
    }

    #[test]
    fn missing_matches_and_full_removal_handle_all_directions() {
        for count in [0, 2, -2] {
            let request = LRemReq {
                key: "list".into(),
                count,
                element: "a".into(),
            };
            let mut list = VecDeque::from([Bytes::from_static(b"b"), Bytes::from_static(b"c")]);
            assert_eq!(request.remove_elements(&mut list), 0);
            assert_eq!(list, VecDeque::from(["b".into(), "c".into()]));
            let mut list = VecDeque::from([Bytes::from_static(b"a"), Bytes::from_static(b"a")]);
            assert_eq!(request.remove_elements(&mut list), 2);
            assert!(list.is_empty());
            assert_eq!(request.remove_elements(&mut list), 0);
        }
    }

    #[test]
    fn minimum_count_removes_from_tail_without_overflow() {
        let list = Arc::new(Mutex::new(VecDeque::from([
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"a"),
        ])));
        let snapshot = EntrySnapshot {
            value: MyValue::new(ValueObject::List(list.clone())),
            expire_at: Some(100),
        };
        let request = LRemReq {
            key: Bytes::from_static(b"list"),
            count: i64::MIN,
            element: Bytes::from_static(b"a"),
        };
        let (operation, response) = request.mutate(snapshot.clone(), 0);
        assert_eq!(response.encode(), b":2\r\n");
        assert_eq!(*list.lock(), VecDeque::from([Bytes::from_static(b"b")]));
        assert!(matches!(operation, MochaOperation::Insert { .. }));
        let request = LRemReq {
            key: Bytes::from_static(b"list"),
            count: 0,
            element: Bytes::from_static(b"b"),
        };
        let (operation, response) = request.mutate(snapshot, 0);
        assert_eq!(response.encode(), b":1\r\n");
        assert!(matches!(operation, MochaOperation::Remove));
    }
}
