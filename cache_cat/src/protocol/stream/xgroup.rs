use std::fmt;
use std::fmt::Formatter;
use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{GroupStart, RedisStream, StreamError};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum XGroupReq {
    Create {
        key: Bytes,
        group: Bytes,
        id: GroupStart,
        entries_read: Option<u64>,
        mkstream: bool,
    },
    SetId {
        key: Bytes,
        group: Bytes,
        id: GroupStart,
        entries_read: Option<u64>,
    },
    Destroy {
        key: Bytes,
        group: Bytes,
    },
    CreateConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
    DelConsumer {
        key: Bytes,
        group: Bytes,
        consumer: Bytes,
    },
}
impl fmt::Display for XGroupReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fn fmt_group_start(id: &GroupStart) -> String {
            match id {
                GroupStart::Id(id) => id.to_string(),
                GroupStart::Last => "$".to_string(),
            }
        }

        match self {
            Self::Create {
                key,
                group,
                id,
                entries_read,
                mkstream,
            } => {
                write!(
                    f,
                    "XGROUP CREATE {} {} {}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(group),
                    fmt_group_start(id)
                )?;

                if *mkstream {
                    write!(f, " MKSTREAM")?;
                }

                if let Some(entries_read) = entries_read {
                    write!(f, " ENTRIESREAD {}", entries_read)?;
                }

                Ok(())
            }

            Self::SetId {
                key,
                group,
                id,
                entries_read,
            } => {
                write!(
                    f,
                    "XGROUP SETID {} {} {}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(group),
                    fmt_group_start(id)
                )?;

                if let Some(entries_read) = entries_read {
                    write!(f, " ENTRIESREAD {}", entries_read)?;
                }

                Ok(())
            }

            Self::Destroy { key, group } => write!(
                f,
                "XGROUP DESTROY {} {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(group)
            ),

            Self::CreateConsumer {
                key,
                group,
                consumer,
            } => write!(
                f,
                "XGROUP CREATECONSUMER {} {} {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(group),
                String::from_utf8_lossy(consumer)
            ),

            Self::DelConsumer {
                key,
                group,
                consumer,
            } => write!(
                f,
                "XGROUP DELCONSUMER {} {} {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(group),
                String::from_utf8_lossy(consumer)
            ),
        }
    }
}

fn arg(v: &Value) -> Result<Bytes, ProtocolError> {
    v.string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("argument"))
}
fn parse_id(v: &Value) -> Result<GroupStart, ProtocolError> {
    let s = String::from_utf8_lossy(&arg(v)?).to_string();
    GroupStart::from_str(&s).map_err(|_| ProtocolError::InvalidArgument("id"))
}
fn parse_num(v: &Value) -> Result<u64, ProtocolError> {
    String::from_utf8_lossy(&arg(v)?)
        .parse()
        .map_err(|_| ProtocolError::InvalidArgument("entries_read"))
}

impl XGroupReq {
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("xgroup"));
        }
        let sub = String::from_utf8_lossy(&arg(&items[1])?).to_ascii_uppercase();
        match sub.as_str() {
            "CREATE" => {
                if items.len() < 5 {
                    return Err(ProtocolError::WrongArgCount("xgroup|create"));
                }
                let key = arg(&items[2])?;
                let group = arg(&items[3])?;
                let id = parse_id(&items[4])?;
                let mut mk = false;
                let mut er = None;
                let mut i = 5;
                while i < items.len() {
                    let o = String::from_utf8_lossy(&arg(&items[i])?).to_ascii_uppercase();
                    match o.as_str() {
                        "MKSTREAM" => mk = true,
                        "ENTRIESREAD" => {
                            i += 1;
                            if i >= items.len() {
                                return Err(ProtocolError::WrongArgCount("xgroup|create"));
                            }
                            er = Some(parse_num(&items[i])?);
                        }
                        _ => return Err(ProtocolError::InvalidArgument("option")),
                    };
                    i += 1;
                }
                Ok(Self::Create {
                    key,
                    group,
                    id,
                    entries_read: er,
                    mkstream: mk,
                })
            }
            "SETID" => {
                if items.len() < 5 {
                    return Err(ProtocolError::WrongArgCount("xgroup|setid"));
                }
                let mut er = None;
                if items.len() > 5 {
                    if items.len() != 7
                        || String::from_utf8_lossy(&arg(&items[5])?).to_ascii_uppercase()
                            != "ENTRIESREAD"
                    {
                        return Err(ProtocolError::InvalidArgument("option"));
                    }
                    er = Some(parse_num(&items[6])?);
                }
                Ok(Self::SetId {
                    key: arg(&items[2])?,
                    group: arg(&items[3])?,
                    id: parse_id(&items[4])?,
                    entries_read: er,
                })
            }
            "DESTROY" => {
                if items.len() != 4 {
                    return Err(ProtocolError::WrongArgCount("xgroup|destroy"));
                }
                Ok(Self::Destroy {
                    key: arg(&items[2])?,
                    group: arg(&items[3])?,
                })
            }
            "CREATECONSUMER" => {
                if items.len() != 5 {
                    return Err(ProtocolError::WrongArgCount("xgroup|createconsumer"));
                }
                Ok(Self::CreateConsumer {
                    key: arg(&items[2])?,
                    group: arg(&items[3])?,
                    consumer: arg(&items[4])?,
                })
            }
            "DELCONSUMER" => {
                if items.len() != 5 {
                    return Err(ProtocolError::WrongArgCount("xgroup|delconsumer"));
                }
                Ok(Self::DelConsumer {
                    key: arg(&items[2])?,
                    group: arg(&items[3])?,
                    consumer: arg(&items[4])?,
                })
            }
            _ => Err(ProtocolError::InvalidArgument("unknown xgroup subcommand")),
        }
    }
}

impl ComputeCommand for XGroupReq {
    fn key(&self) -> &Bytes {
        match self {
            Self::Create { key, .. }
            | Self::SetId { key, .. }
            | Self::Destroy { key, .. }
            | Self::CreateConsumer { key, .. }
            | Self::DelConsumer { key, .. } => key,
        }
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XGroup(self)
    }
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let version = entry.value.version;
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let mut s = stream.write();
        let out = match self {
            Self::Create {
                group,
                id,
                entries_read,
                ..
            } => s
                .xgroup_create(&group, id, entries_read)
                .map(|_| Value::ok()),
            Self::SetId {
                group,
                id,
                entries_read,
                ..
            } => s
                .xgroup_setid(&group, id, entries_read)
                .map(|_| Value::ok()),
            Self::Destroy { group, .. } => {
                Ok(Value::Integer(if s.xgroup_destroy(&group) { 1 } else { 0 }))
            }
            Self::CreateConsumer {
                group, consumer, ..
            } => s
                .xgroup_createconsumer(&group, &consumer)
                .map(|v| Value::Integer(if v { 1 } else { 0 })),
            Self::DelConsumer {
                group, consumer, ..
            } => s
                .xgroup_delconsumer(&group, &consumer)
                .map(|v| Value::Integer(v as i64)),
        };
        drop(s);
        match out {
            Ok(v) => (
                MochaOperation::Insert {
                    value: MyValue {
                        version,
                        data: ValueObject::Stream(stream),
                    },
                    expire,
                },
                v,
            ),
            Err(e) => (MochaOperation::Abort, stream_err(e)),
        }
    }
    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let Self::Create {
            group,
            id,
            entries_read,
            mkstream,
            ..
        } = self
        else {
            return (
                MochaOperation::Abort,
                ProtocolError::response("NOGROUP No such key").into(),
            );
        };
        if !mkstream {
            return (
                MochaOperation::Abort,
                ProtocolError::response("ERR The XGROUP subcommand requires the key to exist")
                    .into(),
            );
        }
        let mut s = RedisStream::new();
        let r = s.xgroup_create(&group, id, entries_read);
        match r {
            Ok(()) => (
                MochaOperation::Insert {
                    value: MyValue::new(ValueObject::Stream(Arc::new(RwLock::new(s)))),
                    expire: ExpirePolicy::Persistent,
                },
                Value::ok(),
            ),
            Err(e) => (MochaOperation::Abort, stream_err(e)),
        }
    }
}
fn stream_err(e: StreamError) -> Value {
    ProtocolError::response(match e {
        StreamError::GroupExists => "BUSYGROUP Consumer Group name already exists",
        StreamError::NoGroup => "NOGROUP No such key or consumer group",
        _ => "ERR stream group error",
    })
    .into()
}

pub struct XGroupCommand;
impl RaftCommand for XGroupCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::XGroup(XGroupReq::parse(
            items,
        )?)))
    }
}
#[async_trait]
impl Command for XGroupCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let op = self.raft_request(items)?;
        if let Some(q) = client.transaction_queue.as_mut() {
            q.push(op);
            return Ok(Value::queued());
        }
        server.app.write(op, client.db_number).await
    }
}
