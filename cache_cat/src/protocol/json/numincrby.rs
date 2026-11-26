//! JSON.NUMINCRBY key path number

use super::format::JsonFormatting;
use super::get::bytes_arg;
use super::path::{JsonPath, PathSource, WriteTarget};
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
use ijson::IValue;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::{cell::LazyCell, fmt};

mod arithmetic;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonNumIncrByReq {
    key: Bytes,
    path: Bytes,
    increment: Bytes,
    pub(crate) resp3: bool,
}

impl fmt::Display for JsonNumIncrByReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.NUMINCRBY {} {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.path),
            String::from_utf8_lossy(&self.increment)
        )
    }
}

enum IncrementReply {
    Last(Number),
    All(Vec<Option<Number>>),
}

impl IncrementReply {
    fn encode(self, resp3: bool) -> Value {
        let format = JsonFormatting::default();
        let json = match self {
            Self::Last(number) => format.encode(&number),
            Self::All(values) if resp3 => {
                return Value::Array(Some(
                    values
                        .into_iter()
                        .map(|value| match value {
                            None => Value::Null,
                            Some(n) => match n.as_i64() {
                                Some(i) => Value::Integer(i),
                                None => Value::Double(n.as_f64().expect("finite JSON number")),
                            },
                        })
                        .collect(),
                ));
            }
            Self::All(values) => format.encode(&values),
        };
        Value::BulkString(Some(json.expect("JSON numbers serialize to bytes").into()))
    }
}

impl JsonNumIncrByReq {
    fn parse(items: &[Value], resp3: bool) -> Result<Self, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("json.numincrby"));
        }
        Ok(Self {
            key: Bytes::copy_from_slice(&bytes_arg(&items[1])?),
            path: bytes_arg(&items[2])?,
            increment: bytes_arg(&items[3])?,
            resp3,
        })
    }

    // Arity is checked before MULTI queues the command. RedisJSON decodes its
    // arguments at execution time, before checking the key.
    fn arguments(&self) -> Result<(&str, &str), Value> {
        let invalid = |_| Value::error("Couldn't parse as UTF-8 string");
        Ok((
            std::str::from_utf8(&self.path).map_err(invalid)?,
            std::str::from_utf8(&self.increment).map_err(invalid)?,
        ))
    }

    fn operand(text: &str) -> Result<Number, Value> {
        match serde_json::from_str(text) {
            Ok(serde_json::Value::Number(n)) => Ok(n),
            Ok(_) => Err(Value::error("bad input number")),
            Err(err) => Err(Value::error(format!("ERR {err}"))),
        }
    }

    fn increment_matches(
        &self,
        path: &JsonPath,
        increment: &str,
        root: &mut IValue,
        changed: &mut bool,
    ) -> Result<IncrementReply, Value> {
        let all = self.resp3 || !path.is_legacy_syntax();
        let locations = path
            .filtered_locations(root, all, IValue::is_number)
            .map_err(|err| Value::error(err.to_string()))?;
        // Redis ignores the operand when the selection contains no numbers.
        let operand = LazyCell::new(|| Self::operand(increment));
        let mut results = locations.into_iter().map(|location| {
            let Some(location) = location else {
                return Ok(None);
            };
            let by = operand.as_ref().map_err(Clone::clone)?;
            let n = match location.target(root) {
                WriteTarget::Value(slot) => arithmetic::increment_value(slot, by)?,
                WriteTarget::Element(array, index) => {
                    arithmetic::increment_element(array, index, by)?
                }
            };
            *changed = true;
            Ok(Some(n))
        });
        if all {
            results.collect::<Result<_, _>>().map(IncrementReply::All)
        } else {
            results
                .try_fold(None, |_, result| result)?
                .map(IncrementReply::Last)
                .ok_or_else(|| {
                    Value::error("ERR Path does not exist or does not contains a number")
                })
        }
    }
}

impl ComputeCommand for JsonNumIncrByReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::JsonNumIncrBy(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (path, increment) = match self.arguments() {
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
        let result =
            self.increment_matches(&path, increment, document.lock().value_mut(), &mut changed);
        // An error after a successful increment must publish the partial write
        // and record its snapshot revision, just like an entirely successful write.
        let update = if changed {
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: entry.get_expire_policy(),
            }
        } else {
            MochaOperation::Abort
        };
        (
            update,
            result.map_or_else(|err| err, |reply| reply.encode(self.resp3)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let error = match self.arguments() {
            Err(err) => err,
            Ok(_) => {
                Value::error("ERR could not perform this operation on a key that doesn't exist")
            }
        };
        (MochaOperation::Abort, error)
    }
}

pub struct JsonNumIncrByCommand;

impl RaftCommand for JsonNumIncrByCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(
            JsonNumIncrByReq::parse(items, false)?.into_base_op(),
        ))
    }
}

#[async_trait]
impl Command for JsonNumIncrByCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = JsonNumIncrByReq::parse(items, client.framed.codec().proto_version() == 3)?;
        let operation = Operation::Base(params.into_base_op());
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(operation);
            return Ok(Value::queued());
        }
        server.app.write(operation, client.db_number).await
    }
}
