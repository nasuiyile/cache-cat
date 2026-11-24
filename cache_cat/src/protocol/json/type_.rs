//! JSON.TYPE key [path]

use super::get::{bytes_arg, text_arg};
use super::path::PathSource;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use ijson::{DestructuredRef, IValue};
use serde::{Deserialize, Serialize};
use std::{fmt, ops::ControlFlow};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonTypeParams {
    key: Bytes,
    path: String,
    pub(crate) resp3: bool,
}

impl fmt::Display for JsonTypeParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.TYPE {} {}",
            String::from_utf8_lossy(&self.key),
            self.path
        )
    }
}

fn type_name(value: &IValue) -> Value {
    let name: &'static [u8] = match value.destructure_ref() {
        DestructuredRef::Null => b"null",
        DestructuredRef::Bool(_) => b"boolean",
        DestructuredRef::Number(n) if n.has_decimal_point() => b"number",
        DestructuredRef::Number(_) => b"integer",
        DestructuredRef::String(_) => b"string",
        DestructuredRef::Array(_) => b"array",
        DestructuredRef::Object(_) => b"object",
    };
    Value::BulkString(Some(Bytes::from_static(name)))
}

impl JsonTypeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        let key = bytes_arg(
            items
                .get(1)
                .ok_or(ProtocolError::WrongArgCount("json.type"))?,
        )?;
        // RedisJSON reads just one optional UTF-8 path and defaults to legacy root.
        let path = items
            .get(2)
            .and_then(|arg| text_arg(arg).ok())
            .unwrap_or_else(|| ".".into());
        Ok(Self {
            key,
            path,
            resp3: false,
        })
    }

    fn query(&self, entry: Option<EntrySnapshot<MyValue>>) -> Result<Value, ProtocolError> {
        let document = match entry.as_ref().map(|entry| &entry.value.data) {
            Some(ValueObject::Json(document)) => Ok(Some(document)),
            Some(_) => Err(ProtocolError::response("Existing key has wrong Redis type")),
            None => Ok(None),
        };
        let source = PathSource::new(&self.path);
        // RedisJSON rejects legacy projections even on a missing or non-JSON key.
        // Modern paths are compiled only when the key holds a JSON document.
        let path = if source.is_legacy() || matches!(document, Ok(Some(_))) {
            match source.parse() {
                Ok(path) if path.is_projection() => {
                    return Err(ProtocolError::response(
                        "ERR computed/projection expressions are only supported by JSON.GET/JSON.MGET/JSON.RESP",
                    ));
                }
                Ok(path) => Some(path),
                Err(_) if source.is_legacy() => None,
                Err(err) => return Err(ProtocolError::response(err.to_string())),
            }
        } else {
            None
        };
        let (Some(document), Some(path)) = (document?, path) else {
            return Ok(Value::Null);
        };
        let document = document.lock();
        if source.is_legacy() {
            let first = path.visit_matches(document.value(), |node| {
                ControlFlow::Break(type_name(node.as_value()))
            });
            Ok(first.break_value().unwrap_or(Value::Null))
        } else {
            let mut types = Vec::new();
            let _ = path.visit_matches(document.value(), |node| {
                types.push(type_name(node.as_value()));
                ControlFlow::<std::convert::Infallible>::Continue(())
            });
            Ok(Value::Array(Some(types)))
        }
    }
}

impl ReadCommand for JsonTypeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        match self.query(value) {
            Ok(reply) if self.resp3 => Value::Array(Some(vec![reply])),
            Ok(reply) => reply,
            Err(err) => err.into(),
        }
    }
}

pub struct JsonTypeCommand;

impl ReadRaftCommand for JsonTypeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::JsonType(JsonTypeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for JsonTypeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut params = JsonTypeParams::parse(items)?;
        params.resp3 = client.framed.codec().proto_version() == 3;
        let operation = ReadOperation::JsonType(params);
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Read(operation));
            return Ok(Value::queued());
        }
        server.app.read(operation, client.db_number).await
    }
}
