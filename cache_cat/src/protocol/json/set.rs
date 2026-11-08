use super::path::{JsonPath, SetMode};
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::json::JsonDocument;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum InputFormat {
    Json,
    Bson,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum FloatArray {
    Bf16,
    Fp16,
    Fp32,
    Fp64,
}

/// Replicate the supplied bytes, not a second long-lived parsed document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonSetReq {
    pub key: Bytes,
    pub path: String,
    pub json: Bytes,
    pub mode: SetMode,
    format: InputFormat,
    float_array: Option<FloatArray>,
}

impl fmt::Display for JsonSetReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.SET {} {} ({} bytes)",
            String::from_utf8_lossy(&self.key),
            self.path,
            self.json.len()
        )
    }
}

impl JsonSetReq {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("json.set"));
        }
        // RESP arguments can share a receive buffer. The persisted key must
        // not retain that buffer after its JSON payload becomes a compact tree.
        let key = Bytes::copy_from_slice(&super::get::bytes_arg(&items[1])?);
        let path = super::get::text_arg(&items[2])?;
        let json = super::get::bytes_arg(&items[3])?;
        let mut req = Self {
            key,
            path,
            json,
            mode: SetMode::Always,
            format: InputFormat::Json,
            float_array: None,
        };
        let mut index = 4;
        while index < items.len() {
            let option = super::get::text_arg(&items[index])?;
            if option.eq_ignore_ascii_case("NX") && req.mode == SetMode::Always {
                req.mode = SetMode::Nx;
            } else if option.eq_ignore_ascii_case("XX") && req.mode == SetMode::Always {
                req.mode = SetMode::Xx;
            } else if option.eq_ignore_ascii_case("FORMAT") {
                index += 1;
                let value = items
                    .get(index)
                    .ok_or(ProtocolError::WrongArgCount("json.set"))?;
                req.format = match super::get::text_arg(value)?.as_str() {
                    "JSON" | "STRING" => InputFormat::Json,
                    "BSON" => InputFormat::Bson,
                    _ => return Err(ProtocolError::response("ERR wrong format")),
                };
            } else if option.eq_ignore_ascii_case("FPHA") {
                index += 1;
                let value = items
                    .get(index)
                    .ok_or(ProtocolError::WrongArgCount("json.set"))?;
                req.float_array = Some(
                    match super::get::text_arg(value)?.to_ascii_uppercase().as_str() {
                        "BF16" => FloatArray::Bf16,
                        "FP16" => FloatArray::Fp16,
                        "FP32" => FloatArray::Fp32,
                        "FP64" => FloatArray::Fp64,
                        _ => return Err(ProtocolError::response("ERR invalid FPHA type")),
                    },
                );
            } else {
                return Err(ProtocolError::SyntaxError);
            }
            index += 1;
        }
        Ok(req)
    }

    fn document(&self) -> Result<JsonDocument, Value> {
        let fp = self.float_array.map(|kind| match kind {
            FloatArray::Bf16 => ijson::FloatType::BF16,
            FloatArray::Fp16 => ijson::FloatType::F16,
            FloatArray::Fp32 => ijson::FloatType::F32,
            FloatArray::Fp64 => ijson::FloatType::F64,
        });
        match self.format {
            InputFormat::Json => JsonDocument::parse_with_float_type(&self.json, fp)
                .map_err(|err| Value::error(format!("ERR {err}"))),
            InputFormat::Bson => {
                std::str::from_utf8(&self.json).map_err(|_| Value::error("ERR invalid UTF-8"))?;
                let document: bson::Document = bson::from_slice(&self.json)
                    .map_err(|err| Value::error(format!("ERR {err}")))?;
                // Redis's BSON input is an envelope: its first field is the
                // JSON root, and an empty document represents JSON null.
                let value = document
                    .into_iter()
                    .next()
                    .map(|(_, value)| value)
                    .unwrap_or(bson::Bson::Null);
                let bytes = serde_json::to_vec(&value.into_relaxed_extjson())
                    .map_err(|err| Value::error(format!("ERR {err}")))?;
                JsonDocument::parse_with_float_type(&bytes, fp)
                    .map_err(|err| Value::error(format!("ERR {err}")))
            }
        }
    }
}

impl ComputeCommand for JsonSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::JsonSet(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let ValueObject::Json(document) = &entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let replacement = match self.document() {
            Ok(value) => value,
            Err(err) => return (MochaOperation::Abort, err),
        };
        let path = match JsonPath::parse(&self.path) {
            Ok(path) => path,
            Err(err) => return (MochaOperation::Abort, Value::error(format!("ERR {err}"))),
        };
        let mut document = document.lock();
        if self.path == "$" || self.path == "." {
            if self.mode == SetMode::Nx {
                return (MochaOperation::Abort, Value::Null);
            }
            // Transfer the parsed tree when replacing the root; cloning it
            // here would temporarily double the new document's allocations.
            *document = replacement;
            drop(document);
            return (
                MochaOperation::Insert {
                    value: entry.value.clone(),
                    expire: entry.get_expire_policy(),
                },
                Value::ok(),
            );
        }
        let outcome = match path.set(document.value_mut(), replacement.value(), self.mode) {
            Ok(outcome) => outcome,
            Err(err) => return (MochaOperation::Abort, Value::error(format!("ERR {err}"))),
        };
        drop(document);
        let response = if outcome.complete {
            Value::ok()
        } else {
            Value::Null
        };
        if !outcome.changed {
            return (MochaOperation::Abort, response);
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: entry.get_expire_policy(),
            },
            response,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let document = match self.document() {
            Ok(value) => value,
            Err(err) => return (MochaOperation::Abort, err),
        };
        if self.mode == SetMode::Xx {
            return (MochaOperation::Abort, Value::Null);
        }
        // Redis only permits creation at the literal root; absent-key paths
        // must not implicitly create intermediate objects.
        if self.path != "$" && self.path != "." {
            return (
                MochaOperation::Abort,
                Value::error("ERR new objects must be created at the root"),
            );
        }
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Json(Arc::new(Mutex::new(document)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::ok(),
        )
    }
}

pub struct JsonSetCommand;

impl RaftCommand for JsonSetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::JsonSet(JsonSetReq::parse(
            items,
        )?)))
    }
}

#[async_trait]
impl Command for JsonSetCommand {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_key_does_not_keep_the_unparsed_receive_buffer_alive() {
        let received = Bytes::from(vec![b'x'; 1024 * 1024]);
        let key = received.slice(..3);
        let args = [
            Value::BulkString(Some(Bytes::from_static(b"JSON.SET"))),
            Value::BulkString(Some(key.clone())),
            Value::BulkString(Some(Bytes::from_static(b"$"))),
            Value::BulkString(Some(Bytes::from_static(b"{}"))),
        ];
        let request = JsonSetReq::parse(&args).unwrap();
        assert_eq!(request.key, key);
        assert_ne!(request.key.as_ptr(), key.as_ptr());
    }
}
