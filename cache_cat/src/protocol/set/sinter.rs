use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::MultiReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for SINTER command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SInterParams {
    pub keys: Vec<Bytes>,
}

impl Display for SInterParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SINTER")?;
        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }
        Ok(())
    }
}

impl SInterParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("sinter"));
        }

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < items.len() - 1 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        Ok(SInterParams { keys })
    }
}

/// SINTER command executor
pub struct SInterCommand;

impl ReadRaftCommand for SInterCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SInter(SInterParams::parse(items)?))
    }
}

#[async_trait]
impl Command for SInterCommand {
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

impl MultiReadCommand for SInterParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(&self, values: Vec<Option<EntrySnapshot<MyValue>>>) -> Value {
        // Source members are unique; filtering needs no temporary hash index.
        let mut results: Option<Vec<Bytes>> = None;

        for value in values {
            let Some(value) = value else {
                // the key not exists, set empty.
                match results {
                    None => results = Some(Default::default()),
                    Some(ref mut set) => set.clear(),
                }

                continue;
            };

            let ValueObject::Set(data) = value.value.data else {
                return ProtocolError::WrongType.into();
            };

            match results {
                // init the result set
                None => results = Some(data.lock().iter().cloned().collect()),

                // continue for check the value types
                // and skip the lock
                Some(ref set) if set.is_empty() => continue,

                Some(ref mut set) => {
                    let data = data.lock();
                    set.retain(|v| data.contains(v));
                }
            }
        }

        let members = results
            .map(|res| {
                res.into_iter()
                    .map(|v| Value::BulkString(Some(v)))
                    .collect()
            })
            .unwrap_or_default();

        // Set reply (RESP2 *N, RESP3 ~N); empty when nothing intersects.
        Value::Set(members)
    }

    fn execute_with_clock(
        &self,
        values: Vec<Option<EntrySnapshot<MyValue>>>,
        _read_clock: u64,
    ) -> Value {
        super::canonical_set_reply(self.execute(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::scan_set::ScanSet;
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn set_entry(members: &[&'static [u8]]) -> EntrySnapshot<MyValue> {
        let members: ScanSet<_> = members
            .iter()
            .map(|member| Bytes::from_static(member))
            .collect();
        EntrySnapshot {
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(members)))),
            expire_at: None,
        }
    }

    #[test]
    fn wrong_type_is_reported_even_when_first_key_is_missing() {
        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        };
        let params = SInterParams {
            keys: vec![
                Bytes::from_static(b"missing"),
                Bytes::from_static(b"string"),
            ],
        };
        assert_eq!(
            params.execute(vec![None, Some(wrong_type)]).encode(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn repeated_keys_filter_binary_members_without_mutating_sources() {
        let source = set_entry(&[b"", b"\0", b"\xff", b"a"]);
        let other = set_entry(&[b"\0", b"\xff", b"other"]);
        let params = SInterParams {
            keys: vec!["source".into(), "source".into(), "other".into()],
        };
        let values = vec![Some(source.clone()), Some(source.clone()), Some(other)];
        let expected = Value::Set(vec![
            Value::BulkString(Some(Bytes::from_static(b"\0"))),
            Value::BulkString(Some(Bytes::from_static(b"\xff"))),
        ]);
        for reply in [
            super::super::canonical_set_reply(params.execute(values.clone())),
            params.execute_with_clock(values, 42),
        ] {
            for proto in [2, 3] {
                assert_eq!(reply.encode_proto(proto), expected.encode_proto(proto));
            }
        }
        let ValueObject::Set(original) = &source.value.data else {
            unreachable!();
        };
        assert_eq!(original.lock().len(), 4);

        let wrong_type = EntrySnapshot {
            value: MyValue::new(ValueObject::Int(1)),
            expire_at: None,
        };
        let params = SInterParams {
            keys: vec!["source".into(), "disjoint".into(), "wrong".into()],
        };
        let values = vec![
            Some(source),
            Some(set_entry(&[b"disjoint"])),
            Some(wrong_type),
        ];
        for reply in [
            params.execute(values.clone()),
            params.execute_with_clock(values, 42),
        ] {
            assert_eq!(
                reply.encode(),
                Value::from(ProtocolError::WrongType).encode()
            );
        }
    }
}
