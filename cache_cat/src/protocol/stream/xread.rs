//! Redis 8.10 XREAD, including cumulative MAXCOUNT and RESP-byte MAXSIZE budgets.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::ReadRaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{ReadStart, StreamError, StreamId};
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{fmt, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XReadId {
    After(StreamId),
    Tail,
    Latest,
}

impl XReadId {
    fn start(self) -> ReadStart {
        match self {
            Self::After(id) => ReadStart::After(id),
            Self::Tail => ReadStart::Tail,
            Self::Latest => ReadStart::Latest,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadParams {
    pub keys: Vec<Bytes>,
    pub ids: Vec<XReadId>,
    pub count: Option<usize>,
    pub max_count: Option<usize>,
    pub max_size: Option<usize>,
    pub block_ms: Option<u64>,
    /// MAXSIZE depends on the negotiated wire format, including inside EXEC.
    pub resp_version: u8,
}

impl fmt::Display for XReadParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "XREAD")?;
        for (name, limit) in [
            ("COUNT", self.count),
            ("MAXCOUNT", self.max_count),
            ("MAXSIZE", self.max_size),
        ] {
            if let Some(limit) = limit {
                write!(f, " {name} {limit}")?;
            }
        }
        if let Some(ms) = self.block_ms {
            write!(f, " BLOCK {ms}")?;
        }
        write!(f, " STREAMS")?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        for id in &self.ids {
            match id {
                XReadId::After(id) => write!(f, " {id}")?,
                XReadId::Tail => write!(f, " $")?,
                XReadId::Latest => write!(f, " +")?,
            }
        }
        Ok(())
    }
}

fn stream_error(error: StreamError) -> ProtocolError {
    ProtocolError::response(format!("ERR {error}"))
}

/// Redis's string2ll accepts canonical signed decimal integers only.
fn integer(value: &Value) -> Result<i64, ProtocolError> {
    let text = value.as_str_lossy().ok_or(ProtocolError::NotAnInteger)?;
    let number = text
        .parse::<i64>()
        .map_err(|_| ProtocolError::NotAnInteger)?;
    if number.to_string() != text {
        return Err(ProtocolError::NotAnInteger);
    }
    Ok(number)
}

impl XReadParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("xread"));
        }
        let mut params = Self {
            keys: Vec::new(),
            ids: Vec::new(),
            count: None,
            max_count: None,
            max_size: None,
            block_ms: None,
            resp_version: 2,
        };
        let mut index = 1;
        let streams = loop {
            let option = items
                .get(index)
                .and_then(Value::as_str_lossy)
                .ok_or(ProtocolError::SyntaxError)?;
            if option.eq_ignore_ascii_case("STREAMS") {
                let streams = &items[index + 1..];
                if streams.is_empty() {
                    return Err(ProtocolError::SyntaxError);
                }
                if !streams.len().is_multiple_of(2) {
                    return Err(ProtocolError::response(
                        "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.",
                    ));
                }
                break streams;
            }
            let argument = items.get(index + 1).ok_or(ProtocolError::SyntaxError)?;
            match option.to_ascii_uppercase().as_str() {
                "COUNT" => {
                    let count = integer(argument)?;
                    params.count = if count > 0 {
                        Some(usize::try_from(count).map_err(|_| ProtocolError::NotAnInteger)?)
                    } else {
                        None
                    };
                }
                "MAXCOUNT" | "MAXSIZE" => {
                    let limit = integer(argument)?;
                    if limit <= 0 {
                        return Err(ProtocolError::response(format!(
                            "ERR {} must be a positive integer",
                            option.to_ascii_uppercase()
                        )));
                    }
                    let limit =
                        Some(usize::try_from(limit).map_err(|_| ProtocolError::NotAnInteger)?);
                    if option.eq_ignore_ascii_case("MAXCOUNT") {
                        params.max_count = limit;
                    } else {
                        params.max_size = limit;
                    }
                }
                "BLOCK" => {
                    let ms = integer(argument).map_err(|_| {
                        ProtocolError::response("ERR timeout is not an integer or out of range")
                    })?;
                    if ms < 0 {
                        return Err(ProtocolError::response("ERR timeout is negative"));
                    }
                    if tokio::time::Instant::now()
                        .checked_add(Duration::from_millis(ms as u64))
                        .is_none()
                    {
                        return Err(ProtocolError::response("ERR timeout is out of range"));
                    }
                    params.block_ms = Some(ms as u64);
                }
                _ => return Err(ProtocolError::SyntaxError),
            }
            index += 2;
        };
        if matches!((params.count, params.max_count), (Some(count), Some(max)) if max < count) {
            return Err(ProtocolError::response(
                "ERR MAXCOUNT must be greater than or equal to COUNT",
            ));
        }
        let num_keys = streams.len() / 2;
        params.keys = streams[..num_keys]
            .iter()
            .map(|value| {
                value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("key"))
            })
            .collect::<Result<_, _>>()?;
        params.ids = streams[num_keys..].iter().map(|value| {
            let id = value.as_str_lossy().ok_or(ProtocolError::InvalidArgument("id"))?;
            match id.as_ref() {
                "$" => Ok(XReadId::Tail),
                "+" => Ok(XReadId::Latest),
                ">" => Err(ProtocolError::response("ERR The > ID can be specified only when calling XREADGROUP using the GROUP <group> <consumer> option.")),
                _ => id.parse().map(XReadId::After).map_err(|_| ProtocolError::response("ERR Invalid stream ID specified as stream command argument")),
            }
        }).collect::<Result<_, _>>()?;
        Ok(params)
    }
}

pub struct XReadCommand;

impl ReadRaftCommand for XReadCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XRead(XReadParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XReadCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}

impl MultiReadCommand for XReadParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    /// Transactions and Lua use this synchronous path and never block.
    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        todo!()
    }
}
