//! XADD command implementation.

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::{AddId, Fields, RedisStream, SharedStream};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parsed parameters for the basic XADD form.
///
/// `XADD key id field value [field value ...]`
#[derive(Debug, Clone, PartialEq)]
pub struct XAddParams {
    /// Key that holds the stream.
    pub key: Bytes,
    /// Parsed entry ID, for example `*` or `1234567890-0`.
    pub id: AddId,
    /// Ordered, binary-safe field-value pairs.
    pub fields: Fields,
}

impl XAddParams {
    /// Parse the required XADD arguments.
    ///
    /// Stream trimming and `NOMKSTREAM` options will be added together with
    /// the storage execution path.
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: XADD key id field value
        if items.len() < 5 || !(items.len() - 3).is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("xadd"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let id = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("id"))?
            .parse()
            .map_err(|_| ProtocolError::InvalidArgument("id"))?;

        let fields = items[3..]
            .chunks_exact(2)
            .map(|pair| {
                let field = pair[0]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("field"))?;
                let value = pair[1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("value"))?;
                Ok((field.to_vec(), value.to_vec()))
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?;

        Ok(Self { key, id, fields })
    }
}

/// XADD command handler.
pub struct XAddCommand;

impl RaftCommand for XAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = XAddParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::XAdd(XAddReq {
            key: params.key,
            id: params.id,
            fields: params.fields,
        })))
    }
}

#[async_trait]
impl Command for XAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        server
            .app
            .write(self.raft_request(items)?, client.db_number)
            .await
    }
}

/// Replicated XADD write request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XAddReq {
    pub key: Bytes,
    pub id: AddId,
    pub fields: Fields,
}

impl fmt::Display for XAddReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "XAddReq {{ key: {}, id: {:?}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.id,
            self.fields,
        )
    }
}

impl ComputeCommand for XAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::XAdd(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();
        let version = entry.value.version;
        let ValueObject::Stream(stream) = entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        //使用write_clock 确保所有节点确定性的执行。
        let add_id = match self.id {
            AddId::Auto => AddId::AutoSequence(write_clock),
            _ => self.id,
        };
        let id = match stream.xadd(add_id, self.fields) {
            Ok(id) => id,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(format!("ERR {error}")).into(),
                );
            }
        };

        (
            MochaOperation::Insert {
                value: MyValue {
                    version,
                    data: ValueObject::Stream(stream),
                },
                expire,
            },
            Value::BulkString(Some(id.to_string().into())),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut stream = RedisStream::new();
        let id = match stream.xadd(self.id, self.fields) {
            Ok(id) => id,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    ProtocolError::response(format!("ERR {error}")).into(),
                );
            }
        };
        let value = MyValue::new(ValueObject::Stream(SharedStream::new(stream)));
        (
            MochaOperation::Insert {
                value,
                expire: ExpirePolicy::Persistent,
            },
            Value::BulkString(Some(id.to_string().into())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{XAddCommand, XAddParams, XAddReq};
    use crate::error::ProtocolError;
    use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::mocha::cas::ComputeCommand;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::structure::stream::{AddId, RedisStream, SharedStream, StreamId};
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::bae_operation::BaseOperation;
    use crate::raft::types::entry::request::Operation;
    use bytes::Bytes;

    fn bulk(value: &'static [u8]) -> Value {
        Value::BulkString(Some(Bytes::from_static(value)))
    }

    #[test]
    fn parses_basic_xadd_arguments() {
        let params = XAddParams::parse(&[
            bulk(b"XADD"),
            bulk(b"events"),
            bulk(b"*"),
            bulk(b"type"),
            bulk(b"created"),
        ])
        .unwrap();

        assert_eq!(params.key, "events");
        assert_eq!(params.id, AddId::Auto);
        assert_eq!(params.fields, vec![(b"type".to_vec(), b"created".to_vec())]);
    }

    #[test]
    fn parses_explicit_and_auto_sequence_ids() {
        let explicit = XAddParams::parse(&[
            bulk(b"XADD"),
            bulk(b"events"),
            bulk(b"42-7"),
            bulk(b"type"),
            bulk(b"created"),
        ])
        .unwrap();
        let auto_sequence = XAddParams::parse(&[
            bulk(b"XADD"),
            bulk(b"events"),
            bulk(b"42-*"),
            bulk(b"type"),
            bulk(b"created"),
        ])
        .unwrap();

        assert_eq!(explicit.id, AddId::Explicit(StreamId::new(42, 7)));
        assert_eq!(auto_sequence.id, AddId::AutoSequence(42));
    }

    #[test]
    fn rejects_unpaired_fields() {
        let error = XAddParams::parse(&[bulk(b"XADD"), bulk(b"events"), bulk(b"*"), bulk(b"type")])
            .unwrap_err();

        assert_eq!(error, ProtocolError::WrongArgCount("xadd"));
    }

    #[test]
    fn builds_a_replicated_xadd_request() {
        let operation = XAddCommand
            .raft_request(&[
                bulk(b"XADD"),
                bulk(b"events"),
                bulk(b"*"),
                bulk(b"type"),
                bulk(b"created"),
            ])
            .unwrap();

        let Operation::Base(BaseOperation::XAdd(XAddReq { key, id, fields })) = operation else {
            panic!("expected XADD base operation");
        };
        assert_eq!(key, "events");
        assert_eq!(id, AddId::Auto);
        assert_eq!(fields, vec![(b"type".to_vec(), b"created".to_vec())]);
    }

    #[test]
    fn writes_a_new_stream_and_returns_the_id() {
        let request = XAddReq {
            key: Bytes::from_static(b"events"),
            id: AddId::Explicit(StreamId::new(1, 0)),
            fields: vec![(b"type".to_vec(), b"created".to_vec())],
        };

        let (operation, response) = request.init();

        assert!(matches!(response, Value::BulkString(Some(ref id)) if id == "1-0"));
        let MochaOperation::Insert { value, expire } = operation else {
            panic!("expected inserted stream");
        };
        assert_eq!(expire, ExpirePolicy::Persistent);
        let ValueObject::Stream(stream) = value.data else {
            panic!("expected stream value");
        };
        assert_eq!(
            stream.inspect(|s| s.get(StreamId::new(1, 0))).unwrap().unwrap().fields,
            vec![(b"type".to_vec(), b"created".to_vec())]
        );
    }

    #[test]
    fn appends_to_an_existing_stream_and_preserves_expiry() {
        let mut stream = RedisStream::new();
        stream
            .xadd(
                AddId::Explicit(StreamId::new(1, 0)),
                vec![(b"first".to_vec(), b"entry".to_vec())],
            )
            .unwrap();
        let entry = EntrySnapshot {
            value: MyValue::new(ValueObject::Stream(SharedStream::new(stream))),
            expire_at: Some(42),
        };
        let request = XAddReq {
            key: Bytes::from_static(b"events"),
            id: AddId::Explicit(StreamId::new(2, 0)),
            fields: vec![(b"second".to_vec(), b"entry".to_vec())],
        };

        let (operation, response) = request.mutate(entry, 0);

        assert!(matches!(response, Value::BulkString(Some(ref id)) if id == "2-0"));
        let MochaOperation::Insert { value, expire } = operation else {
            panic!("expected updated stream");
        };
        assert_eq!(expire, ExpirePolicy::Absolute(42));
        let ValueObject::Stream(stream) = value.data else {
            panic!("expected stream value");
        };
        assert_eq!(stream.xlen().unwrap(), 2);
        assert!(stream.inspect(|s| s.contains(StreamId::new(2, 0))).unwrap());
    }
}
