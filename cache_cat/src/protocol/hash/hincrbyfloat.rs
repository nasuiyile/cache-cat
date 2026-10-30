//! HINCRBYFLOAT key field increment

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::float;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::scan_map::ScanMap as HashMap;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rustc_apfloat::{Float, ieee::X87DoubleExtended};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

const INVALID_FLOAT: &str = "ERR value is not a valid float";
const INVALID_HASH_FLOAT: &str = "ERR hash value is not a float";
const NON_FINITE_RESULT: &str = "ERR increment would produce NaN or Infinity";

pub struct HIncrByFloatCommand;

impl RaftCommand for HIncrByFloatCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("hincrbyfloat"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let field = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("field"))?;
        let increment = items[3]
            .string_bytes_clone()
            .ok_or_else(|| ProtocolError::response(INVALID_FLOAT))?;

        Ok(Operation::Base(BaseOperation::HIncrByFloat(
            HIncrByFloatReq {
                key,
                field,
                increment,
            },
        )))
    }
}

#[async_trait]
impl Command for HIncrByFloatCommand {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HIncrByFloatReq {
    pub key: Bytes,
    pub field: Bytes,
    // Redis validates numbers when executing, so even an invalid increment
    // must be queueable in MULTI. Software parsing/arithmetic also ensures
    // identical Raft and snapshot results on every host, without f64 narrowing.
    pub increment: Bytes,
}

impl fmt::Display for HIncrByFloatReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HIncrByFloatReq {{ key: {}, field: {}, increment: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field),
            String::from_utf8_lossy(&self.increment),
        )
    }
}

impl HIncrByFloatReq {
    fn parse_increment(&self) -> Result<X87DoubleExtended, ProtocolError> {
        float::parse(&self.increment).ok_or_else(|| ProtocolError::response(INVALID_FLOAT))
    }

    fn add(old: X87DoubleExtended, increment: X87DoubleExtended) -> Result<Bytes, ProtocolError> {
        let value = (old + increment).value;
        if !value.is_finite() {
            return Err(ProtocolError::response(NON_FINITE_RESULT));
        }
        Ok(float::format(value))
    }
}

impl ComputeCommand for HIncrByFloatReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HIncrByFloat(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let increment = match self.parse_increment() {
            Ok(increment) => increment,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        let ValueObject::Hash(hash) = &entry.value.data else {
            return (MochaOperation::Abort, ProtocolError::WrongType.into());
        };
        let mut map = hash.lock();
        let old = match map.get(&self.field) {
            Some(HashValue::Int(value)) => X87DoubleExtended::from_i128(*value as i128).value,
            Some(HashValue::Str(bytes)) => match float::parse(bytes) {
                Some(value) => value,
                None => {
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response(INVALID_HASH_FLOAT).into(),
                    );
                }
            },
            None => X87DoubleExtended::ZERO,
        };
        let value = match Self::add(old, increment) {
            Ok(value) => value,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        // All validation precedes mutation, including overflow of the sum.
        map.insert(self.field, HashValue::Str(value.clone()));
        drop(map);
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: entry.get_expire_policy(),
            },
            Value::BulkString(Some(value)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let value = match self
            .parse_increment()
            .and_then(|increment| Self::add(X87DoubleExtended::ZERO, increment))
        {
            Ok(value) => value,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        let map = HashMap::from([(self.field, HashValue::Str(value.clone()))]);
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            // Redis returns a bulk string in RESP2 and RESP3 alike.
            Value::BulkString(Some(value)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::hash::hget::HGetParams;
    use crate::protocol::hash::hset::HSetReq;
    use crate::raft::types::core::mocha::read_command::ReadCommand;

    fn args(increment: &[u8]) -> Vec<Value> {
        [b"HINCRBYFLOAT".as_slice(), b"h", b"f", increment]
            .into_iter()
            .map(|bytes| Value::BulkString(Some(Bytes::copy_from_slice(bytes))))
            .collect()
    }

    fn request(increment: &[u8]) -> HIncrByFloatReq {
        let Operation::Base(BaseOperation::HIncrByFloat(request)) =
            HIncrByFloatCommand.raft_request(&args(increment)).unwrap()
        else {
            panic!("expected HINCRBYFLOAT request");
        };
        request
    }

    fn hash(old: &str) -> EntrySnapshot<MyValue> {
        let (MochaOperation::Insert { value, .. }, _) = (HSetReq {
            key: "h".into(),
            elements: vec![
                ("f".into(), Bytes::copy_from_slice(old.as_bytes())),
                ("untouched".into(), "value".into()),
            ],
        })
        .init() else {
            panic!("expected hash");
        };
        EntrySnapshot {
            value,
            expire_at: Some(10_000),
        }
    }

    fn read(hash: &EntrySnapshot<MyValue>, field: &str) -> Value {
        HGetParams {
            key: "h".into(),
            field: Bytes::copy_from_slice(field.as_bytes()),
        }
        .execute(Some(hash.clone()))
    }

    fn assert_bulk(reply: &Value, expected: &str) {
        let expected = format!("${}\r\n{expected}\r\n", expected.len());
        for proto in [2, 3] {
            assert_eq!(reply.encode_proto(proto), expected.as_bytes());
        }
    }

    #[test]
    fn hincrbyfloat_checks_arity_and_keeps_binary_keys_fields_and_increment() {
        for count in [0, 1, 2, 3, 5] {
            let values = vec![Value::BulkString(Some("arg".into())); count];
            assert!(matches!(
                HIncrByFloatCommand.raft_request(&values),
                Err(ProtocolError::WrongArgCount("hincrbyfloat"))
            ));
        }
        let mut values = args(b".125");
        values[1] = Value::BulkString(Some(Bytes::from_static(b"\xff\0key")));
        values[2] = Value::BulkString(Some(Bytes::from_static(b"\0\xfefield")));
        let operation = HIncrByFloatCommand.raft_request(&values).unwrap();
        let encoded = bincode2::serialize(&operation).unwrap();
        let Operation::Base(BaseOperation::HIncrByFloat(request)) =
            bincode2::deserialize(&encoded).unwrap()
        else {
            panic!("expected HINCRBYFLOAT request");
        };
        assert_eq!(request.key.as_ref(), b"\xff\0key");
        assert_eq!(request.field.as_ref(), b"\0\xfefield");
        let field = request.field.clone();
        let (operation, reply) = request.init();
        assert_bulk(&reply, "0.125");
        let MochaOperation::Insert { value, expire } = operation else {
            panic!("expected insertion");
        };
        assert!(matches!(expire, ExpirePolicy::Persistent));
        let ValueObject::Hash(map) = value.data else {
            panic!("expected hash");
        };
        assert_eq!(map.lock()[&field].to_bytes().as_ref(), b"0.125");
    }

    #[test]
    fn hincrbyfloat_preserves_extended_precision_and_ttl() {
        for (old, increment, expected) in [
            ("10.50", "0.1", "10.6"),
            ("0.1", "0.2", "0.3"),
            ("5e3", "2e2", "5200"),
            ("10.5", "-10.5", "0"),
            ("-0", "-0", "0"),
            ("1", "0.00000000000000001", "1.00000000000000001"),
            ("9007199254740993", "1", "9007199254740994"),
            ("9223372036854775807", "1", "9223372036854775808"),
            ("+01.50", "-.5", "1"),
            ("0x1.8p+1", "0X1P-1", "3.5"),
        ] {
            let hash = hash(old);
            let (operation, reply) = request(increment.as_bytes()).mutate(hash.clone(), 0);
            assert!(matches!(
                operation,
                MochaOperation::Insert {
                    expire: ExpirePolicy::Absolute(10_000),
                    ..
                }
            ));
            assert_bulk(&reply, expected);
            assert_bulk(&read(&hash, "f"), expected);
            assert_bulk(&read(&hash, "untouched"), "value");
        }
    }

    #[test]
    fn hincrbyfloat_missing_field_starts_at_zero_and_zero_increment_creates_it() {
        for increment in ["-2.5", "0", "-0", "1e-18"] {
            let hash = hash("42");
            let mut request = request(increment.as_bytes());
            request.field = "missing".into();
            let (operation, reply) = request.mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Insert { .. }));
            assert_bulk(&reply, if increment == "-2.5" { "-2.5" } else { "0" });
            assert_eq!(read(&hash, "missing").encode(), reply.encode());
            assert_bulk(&read(&hash, "f"), "42");
        }
    }

    #[test]
    fn hincrbyfloat_rejects_invalid_numbers_without_creating_or_changing_fields() {
        for invalid in [
            b"".as_slice(),
            b" 1",
            b"1 ",
            b"+",
            b"1.2.3",
            b"1\0",
            b"\xff",
            b"NaN",
            b"nan(payload)",
            b"1e4933",
            b"1e-5000",
        ] {
            let request = request(invalid);
            let (operation, reply) = request.clone().init();
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(reply.encode(), b"-ERR value is not a valid float\r\n");
            let hash = hash("42");
            let (operation, reply) = request.mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(reply.encode(), b"-ERR value is not a valid float\r\n");
            assert_bulk(&read(&hash, "f"), "42");
        }

        for old in ["", "abc", " 1", "1 ", "NaN", "1e4933", "1e-5000"] {
            let hash = hash(old);
            let (operation, reply) = request(b"1").mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(reply.encode(), b"-ERR hash value is not a float\r\n");
            assert_bulk(&read(&hash, "f"), old);
        }
    }

    #[test]
    fn hincrbyfloat_non_finite_results_never_mutate_the_hash() {
        for increment in [b"inf".as_slice(), b"-inf", b"+Infinity"] {
            let (operation, reply) = request(increment).init();
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(
                reply.encode(),
                b"-ERR increment would produce NaN or Infinity\r\n"
            );
            let hash = hash("42");
            let mut request = request(increment);
            request.field = "missing".into();
            let (operation, _) = request.mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert!(matches!(read(&hash, "missing"), Value::BulkString(None)));
        }
        for (old, increment) in [("inf", "1"), ("inf", "-inf"), ("1e4932", "1e4932")] {
            let hash = hash(old);
            let (operation, reply) = request(increment.as_bytes()).mutate(hash.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(
                reply.encode(),
                b"-ERR increment would produce NaN or Infinity\r\n"
            );
            assert_bulk(&read(&hash, "f"), old);
        }
    }

    #[test]
    fn hincrbyfloat_validates_increment_before_key_type() {
        let entry = EntrySnapshot {
            value: MyValue::new(ValueObject::String("value".into())),
            expire_at: Some(10_000),
        };
        for (increment, expected) in [
            ("invalid", "ERR value is not a valid float"),
            (
                "1",
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            (
                "inf",
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
        ] {
            let (operation, reply) = request(increment.as_bytes()).mutate(entry.clone(), 0);
            assert!(matches!(operation, MochaOperation::Abort));
            assert_eq!(reply.encode(), format!("-{expected}\r\n").as_bytes());
        }
    }
}
