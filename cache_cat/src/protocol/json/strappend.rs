//! JSON.STRAPPEND key [path] json-string

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
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::{base_operation::BaseOperation, request::Operation};
use async_trait::async_trait;
use bytes::Bytes;
use ijson::{IString, IValue};
use serde::{Deserialize, Serialize};
use std::{cell::LazyCell, fmt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonStrAppendReq {
    key: Bytes,
    path: Bytes,
    json: Bytes,
}

impl fmt::Display for JsonStrAppendReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.STRAPPEND {} {} ({} bytes)",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.path),
            self.json.len()
        )
    }
}

impl JsonStrAppendReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("json.strappend"));
        }
        let (path, json) = if let Some(json) = items.get(3) {
            (bytes_arg(&items[2])?, bytes_arg(json)?)
        } else {
            (Bytes::from_static(b"."), bytes_arg(&items[2])?)
        };
        Ok(Self {
            key: Bytes::copy_from_slice(&bytes_arg(&items[1])?),
            path,
            json,
        })
    }

    // Decode at execution time so malformed UTF-8 occupies its normal
    // reply slot in EXEC, and check it before looking at the entry.
    fn arguments(&self) -> Result<(&str, &str), Value> {
        let invalid = |_| Value::error("Couldn't parse as UTF-8 string");
        Ok((
            std::str::from_utf8(&self.path).map_err(invalid)?,
            std::str::from_utf8(&self.json).map_err(invalid)?,
        ))
    }

    fn append_matches(
        path: &JsonPath,
        json: &str,
        root: &mut IValue,
        changed: &mut bool,
    ) -> Result<Value, Value> {
        let legacy = path.is_legacy_syntax();
        let locations = path
            .filtered_locations(root, !legacy, IValue::is_string)
            .map_err(|err| Value::error(err.to_string()))?;
        // Redis only parses the JSON argument when at least one string matches.
        let suffix = LazyCell::new(|| match serde_json::from_str(json) {
            Ok(serde_json::Value::String(string)) => Ok(string),
            Ok(_) => Err(Value::error(
                "WRONGTYPE wrong type of path value - expected string",
            )),
            Err(err) => Err(Value::error(format!("ERR {err}"))),
        });
        let mut results = locations.into_iter().map(|location| {
            let Some(location) = location else {
                return Ok(Value::Null);
            };
            let suffix = suffix.as_ref().map_err(Clone::clone)?;
            let slot = location
                .value_mut(root)
                .and_then(IValue::as_string_mut)
                .expect("string appends preserve the type and location of all matches");
            let current = slot.as_str();
            let len = current
                .len()
                .checked_add(suffix.len())
                .filter(|&len| len < u32::MAX as usize)
                .ok_or_else(|| Value::error("ERR string is too long"))?;
            if !suffix.is_empty() {
                *slot = IString::intern(&[current, suffix.as_str()].concat());
            }
            *changed = true;
            Ok(Value::Integer(len as i64))
        });
        if legacy {
            results
                .try_fold(None, |_, result| result.map(Some))?
                .ok_or_else(|| Value::error("ERR Path does not exist or not a string"))
        } else {
            results
                .collect::<Result<_, _>>()
                .map(|values| Value::Array(Some(values)))
        }
    }
}

impl ComputeCommand for JsonStrAppendReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::JsonStrAppend(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (path, json) = match self.arguments() {
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
        let reply = Self::append_matches(&path, json, document.lock().value_mut(), &mut changed)
            .unwrap_or_else(|err| err);
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

pub struct JsonStrAppendCommand;

impl RaftCommand for JsonStrAppendCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(
            JsonStrAppendReq::parse(items)?.into_base_op(),
        ))
    }
}

#[async_trait]
impl Command for JsonStrAppendCommand {
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
