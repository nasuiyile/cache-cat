use super::{
    format::JsonFormatting,
    path::{JsonPath, Node},
};
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
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use std::fmt;

pub(super) fn bytes_arg(value: &Value) -> Result<Bytes, ProtocolError> {
    value
        .string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("argument"))
}

pub(super) fn text_arg(value: &Value) -> Result<String, ProtocolError> {
    let bytes = bytes_arg(value)?;
    std::str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|_| ProtocolError::response("ERR invalid UTF-8"))
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
enum ReplyFormat {
    #[default]
    String,
    Expand,
    Expand1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonGetParams {
    pub key: Bytes,
    pub paths: Vec<String>,
    formatting: JsonFormatting,
    format: ReplyFormat,
    format_before_path: bool,
    pub(crate) resp3: bool,
}

impl fmt::Display for JsonGetParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "JSON.GET {} {:?}",
            String::from_utf8_lossy(&self.key),
            self.paths
        )
    }
}

impl JsonGetParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("json.get"));
        }
        let mut params = Self {
            key: bytes_arg(&items[1])?,
            paths: Vec::with_capacity(1),
            formatting: JsonFormatting::default(),
            format: ReplyFormat::String,
            format_before_path: false,
            resp3: false,
        };
        let mut index = 2;
        while index < items.len() {
            let arg = text_arg(&items[index])?;
            if arg.eq_ignore_ascii_case("NOESCAPE") {
                // Accepted for compatibility; Redis still escapes JSON strings.
            } else if arg.eq_ignore_ascii_case("INDENT")
                || arg.eq_ignore_ascii_case("NEWLINE")
                || arg.eq_ignore_ascii_case("SPACE")
                || arg.eq_ignore_ascii_case("FORMAT")
            {
                index += 1;
                let value = text_arg(
                    items
                        .get(index)
                        .ok_or(ProtocolError::WrongArgCount("json.get"))?,
                )?;
                if arg.eq_ignore_ascii_case("INDENT") {
                    params.formatting.indent = Some(value);
                } else if arg.eq_ignore_ascii_case("NEWLINE") {
                    params.formatting.newline = Some(value);
                } else if arg.eq_ignore_ascii_case("SPACE") {
                    params.formatting.space = Some(value);
                } else {
                    params.format_before_path |= params.paths.is_empty();
                    params.format = match value.as_str() {
                        "STRING" => ReplyFormat::String,
                        "EXPAND" => ReplyFormat::Expand,
                        "EXPAND1" => ReplyFormat::Expand1,
                        _ => return Err(ProtocolError::response("ERR wrong reply format")),
                    };
                }
            } else {
                params.paths.push(arg);
            }
            index += 1;
        }
        if params.paths.is_empty() {
            params.paths.push(".".to_owned());
        }
        Ok(params)
    }

    fn bulk<T: Serialize + ?Sized>(&self, value: &T) -> Result<Value, String> {
        self.formatting
            .encode(value)
            .map(|bytes| Value::BulkString(Some(bytes.into())))
            .map_err(|err| format!("ERR {err}"))
    }

    fn expanded(&self, node: &IValue) -> Result<Value, String> {
        match node.destructure_ref() {
            DestructuredRef::Null => Ok(Value::Null),
            DestructuredRef::Bool(value) => Ok(Value::Boolean(value)),
            DestructuredRef::Number(number) => {
                if !number.has_decimal_point() {
                    if let Some(value) = number.to_i64() {
                        return Ok(Value::Integer(value));
                    }
                }
                Ok(Value::Double(number.to_f64_lossy()))
            }
            _ if matches!(self.format, ReplyFormat::Expand1) => self.bulk(node),
            DestructuredRef::String(value) => Ok(Value::BulkString(Some(Bytes::copy_from_slice(
                value.as_bytes(),
            )))),
            DestructuredRef::Array(array) => array
                .iter()
                .map(|value| self.expanded(value.as_ref()))
                .collect::<Result<Vec<_>, _>>()
                .map(|values| Value::Array(Some(values))),
            DestructuredRef::Object(object) => object
                .iter()
                .map(|(key, value)| {
                    Ok((
                        Value::BulkString(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        self.expanded(value)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Map),
        }
    }

    fn get(&self, root: &IValue) -> Result<Value, String> {
        let expanded = self.resp3 && !matches!(self.format, ReplyFormat::String);
        let mut paths = Vec::with_capacity(self.paths.len());
        for original in &self.paths {
            match JsonPath::parse(original) {
                Ok(path) => paths.push((original.as_str(), path)),
                // Redis's multiple-path STRING reply omits invalid paths.
                Err(_) if self.paths.len() > 1 && !expanded => continue,
                Err(err) => return Err(format!("ERR {err}")),
            }
        }
        if expanded {
            // Expanded replies always have an outer path list, including a
            // single or legacy path, and an inner list of its matches.
            return paths
                .iter()
                .map(|(_, path)| {
                    path.select(root)
                        .iter()
                        .map(|node| self.expanded(node.as_value()))
                        .collect::<Result<Vec<_>, _>>()
                        .map(|nodes| Value::Array(Some(nodes)))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|values| Value::Array(Some(values)));
        }
        let legacy = self
            .paths
            .iter()
            .all(|path| JsonPath::is_legacy_source(path));
        if self.paths.len() == 1 {
            let matches = paths[0].1.select(root);
            if legacy && !paths[0].1.is_projection() {
                let value = matches.first().ok_or("ERR Path does not exist")?;
                self.bulk(value)
            } else {
                self.bulk(&matches)
            }
        } else {
            let mut entries = Vec::with_capacity(paths.len());
            for (original, path) in &paths {
                let mut matches = path.select(root);
                let single = legacy && !path.is_projection();
                if single {
                    if matches.is_empty() {
                        return Err("ERR Path does not exist".to_owned());
                    }
                    matches.truncate(1);
                }
                // Duplicate paths are one object member. Keep argument order
                // deterministic for Lua/EXEC without a randomized hash table.
                if let Some((_, existing, as_single)) =
                    entries.iter_mut().find(|(key, _, _)| key == original)
                {
                    *existing = matches;
                    *as_single = single;
                } else {
                    entries.push((*original, matches, single));
                }
            }
            self.bulk(&PathObject { entries })
        }
    }
}

struct PathObject<'a> {
    entries: Vec<(&'a str, Vec<Node<'a>>, bool)>,
}

impl Serialize for PathObject<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.entries.len()))?;
        for (path, matches, single) in &self.entries {
            if *single {
                map.serialize_entry(path, &matches[0])?;
            } else {
                map.serialize_entry(path, matches)?;
            }
        }
        map.end()
    }
}

impl ReadCommand for JsonGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }
    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        if self.format_before_path && !self.resp3 {
            return Value::error("ERR FORMAT argument is not supported on RESP2");
        }
        match value {
            None => Value::Null,
            Some(entry) => match entry.value.data {
                ValueObject::Json(document) => self
                    .get(document.lock().value())
                    .unwrap_or_else(Value::error),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

pub struct JsonGetCommand;

impl ReadRaftCommand for JsonGetCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::JsonGet(JsonGetParams::parse(items)?))
    }
}

#[async_trait]
impl Command for JsonGetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut params = JsonGetParams::parse(items)?;
        params.resp3 = client.framed.codec().proto_version() == 3;
        let operation = ReadOperation::JsonGet(params);
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Read(operation));
            return Ok(Value::queued());
        }
        server.app.read(operation, client.db_number).await
    }
}
