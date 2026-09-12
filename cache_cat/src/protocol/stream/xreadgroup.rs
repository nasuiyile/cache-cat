use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{GroupRead, StreamError, StreamId};
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XReadGroupParams {
    pub key: Bytes,
    pub group: Bytes,
    pub consumer: Bytes,
    pub mode: GroupRead,
    pub count: Option<usize>,
    pub no_ack: bool,
}

impl fmt::Display for XReadGroupParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XReadGroupReq {{ key: {}, group: {}, consumer: {}, mode: {:?}, count: {}, no_ack: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.group),
            String::from_utf8_lossy(&self.consumer),
            self.mode,
            self.count.unwrap_or(0),
            self.no_ack
        )
    }
}

fn arg(v: &Value) -> Result<Bytes, ProtocolError> {
    v.string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("argument"))
}

impl XReadGroupParams {
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 7 {
            return Err(ProtocolError::WrongArgCount("xreadgroup"));
        }
        if !arg(&items[1])?.eq_ignore_ascii_case(b"GROUP") {
            return Err(ProtocolError::SyntaxError);
        }
        let group = arg(&items[2])?;
        let consumer = arg(&items[3])?;
        let mut count = None;
        let mut no_ack = false;
        let mut i = 4;
        while i < items.len() {
            let o = arg(&items[i])?;
            if o.eq_ignore_ascii_case(b"COUNT") {
                i += 1;
                if i >= items.len() {
                    return Err(ProtocolError::SyntaxError);
                };
                let n = String::from_utf8_lossy(&arg(&items[i])?)
                    .parse::<i64>()
                    .map_err(|_| ProtocolError::NotAnInteger)?;
                if n <= 0 {
                    return Err(ProtocolError::response(
                        "ERR COUNT must be a positive integer",
                    ));
                }
                count = Some(n as usize);
                i += 1;
            } else if o.eq_ignore_ascii_case(b"NOACK") {
                no_ack = true;
                i += 1;
            } else if o.eq_ignore_ascii_case(b"BLOCK") {
                return Err(ProtocolError::response(
                    "ERR BLOCK is not supported for XREADGROUP",
                ));
            } else if o.eq_ignore_ascii_case(b"STREAMS") {
                break;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        if i + 2 >= items.len() {
            return Err(ProtocolError::SyntaxError);
        }
        let key = arg(&items[i + 1])?;
        let id = String::from_utf8_lossy(&arg(&items[i + 2])?).to_string();
        let mode = if id == ">" {
            GroupRead::New
        } else {
            GroupRead::PendingAfter(
                StreamId::from_str(&id).map_err(|_| ProtocolError::InvalidArgument("id"))?,
            )
        };
        Ok(Self {
            key,
            group,
            consumer,
            mode,
            count,
            no_ack,
        })
    }
}

fn encode_entry(id: StreamId, fields: Option<Vec<(Vec<u8>, Vec<u8>)>>) -> Value {
    let vals = fields
        .map(|fs| {
            Value::Array(Some(
                fs.into_iter()
                    .flat_map(|(k, v)| {
                        vec![
                            Value::BulkString(Some(k.into())),
                            Value::BulkString(Some(v.into())),
                        ]
                    })
                    .collect(),
            ))
        })
        .unwrap_or(Value::Array(None));
    Value::Array(Some(vec![
        Value::BulkString(Some(id.to_string().into())),
        vals,
    ]))
}
pub struct XReadGroupCommand;

impl ReadRaftCommand for XReadGroupCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::XReadGroup(XReadGroupParams::parse(items)?))
    }
}

#[async_trait]
impl Command for XReadGroupCommand {
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
        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}

impl MultiReadCommand for XReadGroupParams {
    fn keys(&self) -> &Vec<Bytes> {
        todo!()
    }

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        todo!()
    }
}
fn stream_err(e: StreamError) -> Value {
    ProtocolError::response(match e {
        StreamError::NoGroup => "NOGROUP No such key or consumer group",
        _ => "ERR stream group error",
    })
    .into()
}
