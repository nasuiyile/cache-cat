//! JSON.ARRAPPEND key path value [value ...]

use super::get::bytes_arg;
use super::path::{JsonPath, PathSource};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::json::{JsonDocument, MAX_JSON_DEPTH};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::{base_operation::BaseOperation, request::Operation};
use async_trait::async_trait;
use bytes::Bytes;
use ijson::{DestructuredRef, IValue, TryExtend};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonArrAppendReq {
    key: Bytes,
    path: Bytes,
    values: Vec<Bytes>,
}

impl fmt::Display for JsonArrAppendReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.ARRAPPEND {} {} ({} values)",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.path),
            self.values.len()
        )
    }
}

// For RedisJSON's append depth check, even an empty container adds a level.
fn value_depth(value: &IValue) -> usize {
    match value.destructure_ref() {
        DestructuredRef::Object(object) => 1 + object.values().map(value_depth).max().unwrap_or(0),
        DestructuredRef::Array(array) => {
            1 + array.as_slice_of::<IValue>().map_or(0, |values| {
                values.iter().map(value_depth).max().unwrap_or(0)
            })
        }
        _ => 0,
    }
}

impl JsonArrAppendReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Redis advertises arity -3. An empty value list fails at execution,
        // after checking the path's encoding, and can be queued by MULTI.
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("json.arrappend"));
        }
        Ok(Self {
            key: Bytes::copy_from_slice(&bytes_arg(&items[1])?),
            path: bytes_arg(&items[2])?,
            values: items[3..].iter().map(bytes_arg).collect::<Result<_, _>>()?,
        })
    }

    fn arguments(&self) -> Result<(&str, Vec<IValue>), Value> {
        let invalid = |_| Value::error("Couldn't parse as UTF-8 string");
        let path = std::str::from_utf8(&self.path).map_err(invalid)?;
        if self.values.is_empty() {
            return Err(ProtocolError::WrongArgCount("json.arrappend").into());
        }
        let values = self
            .values
            .iter()
            .map(|value| {
                let json = std::str::from_utf8(value).map_err(invalid)?;
                JsonDocument::parse(json.as_bytes())
                    .map(JsonDocument::into_value)
                    .map_err(|err| Value::error(err.to_string()))
            })
            .collect::<Result<_, _>>()?;
        Ok((path, values))
    }

    fn append_matches(
        path: &JsonPath,
        values: Vec<IValue>,
        root: &mut IValue,
        changed: &mut bool,
    ) -> Result<Value, Value> {
        let legacy = path.is_legacy_syntax();
        let locations = path
            .filtered_locations(root, !legacy, IValue::is_array)
            .map_err(|err| Value::error(err.to_string()))?;
        let last = locations.iter().rposition(Option::is_some);
        let Some(last) = last else {
            return if legacy {
                Err(Value::error("ERR Path does not exist or not an array"))
            } else {
                Ok(Value::Array(Some(vec![Value::Null; locations.len()])))
            };
        };
        let height = values.iter().map(value_depth).max().unwrap_or(0);
        let mut values = values;
        let mut results = locations.into_iter().enumerate().map(|(index, location)| {
            let Some(location) = location else {
                return Ok(Value::Null);
            };
            if location.depth() + 1 + height >= MAX_JSON_DEPTH {
                return Err(Value::error("ERR recursion limit exceeded"));
            }
            let array = location
                .value_mut(root)
                .and_then(IValue::as_array_mut)
                .expect("appending preserves all matched array locations");
            // Move the decoded operands into the last target, including the
            // single-target case. Earlier targets receive independent copies.
            let items = if index == last {
                std::mem::take(&mut values)
            } else {
                values.clone()
            };
            let len = array.len();
            let result = array.try_extend(items);
            *changed |= array.len() != len;
            result.map_err(|err| Value::error(err.to_string()))?;
            Ok(Value::Integer(i64::from(array.len())))
        });
        if legacy {
            results.try_fold(Value::Null, |_, result| result)
        } else {
            results
                .collect::<Result<_, _>>()
                .map(|values| Value::Array(Some(values)))
        }
    }
}

impl ComputeCommand for JsonArrAppendReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::JsonArrAppend(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (path, values) = match self.arguments() {
            Ok(args) => args,
            Err(err) => return (MochaOperation::Abort, err),
        };
        let ValueObject::Json(document) = &entry.value.data else {
            return (
                MochaOperation::Abort,
                Value::error("Existing key has wrong Redis type"),
            );
        };
        let path = match PathSource::new(path).parse() {
            Ok(path) if path.is_projection() => {
                return (
                    MochaOperation::Abort,
                    Value::error(
                        "ERR computed/projection expressions are only supported by JSON.GET/JSON.MGET/JSON.RESP",
                    ),
                );
            }
            Ok(path) => path,
            Err(err) => return (MochaOperation::Abort, Value::error(err.to_string())),
        };
        let mut changed = false;
        let reply = Self::append_matches(&path, values, document.lock().value_mut(), &mut changed)
            .unwrap_or_else(|err| err);
        // Earlier matches may already have been updated when a later one
        // exceeds the depth limit. Those writes need a snapshot revision too.
        let update = if changed {
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: entry.get_expire_policy(),
            }
        } else {
            MochaOperation::Abort
        };
        (update, reply)
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let error = self.arguments().err().unwrap_or_else(|| {
            Value::error("ERR could not perform this operation on a key that doesn't exist")
        });
        (MochaOperation::Abort, error)
    }
}

pub struct JsonArrAppendCommand;

impl RaftCommand for JsonArrAppendCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(
            JsonArrAppendReq::parse(items)?.into_base_op(),
        ))
    }
}

#[async_trait]
impl Command for JsonArrAppendCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let operation = self.raft_request(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(operation);
            return Ok(Value::queued());
        }
        server.app.write(operation, client.db_number).await
    }
}
