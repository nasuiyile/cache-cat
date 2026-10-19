//! INCRBYFLOAT key increment

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::common::float;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use rustc_apfloat::{Float, ieee::X87DoubleExtended};
use serde::{Deserialize, Serialize};
use std::fmt;

const INVALID_FLOAT: &str = "ERR value is not a valid float";
const NON_FINITE_RESULT: &str = "ERR increment would produce NaN or Infinity";

pub struct IncrByFloatCommand;

impl RaftCommand for IncrByFloatCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("incrbyfloat"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let increment = items[2]
            .string_bytes_clone()
            .ok_or_else(|| ProtocolError::response(INVALID_FLOAT))?;
        Ok(Operation::Base(BaseOperation::IncrByFloat(
            IncrByFloatReq { key, increment },
        )))
    }
}

#[async_trait]
impl Command for IncrByFloatCommand {
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
pub struct IncrByFloatReq {
    pub key: Bytes,
    // Validate numbers at execution time so MULTI can queue invalid increments.
    // The shared software float also keeps Raft and snapshot replay deterministic.
    pub increment: Bytes,
}

impl fmt::Display for IncrByFloatReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IncrByFloatReq {{ key: {}, increment: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.increment),
        )
    }
}

impl IncrByFloatReq {
    fn incremented_value(&self, old: X87DoubleExtended) -> Result<Bytes, ProtocolError> {
        let increment =
            float::parse(&self.increment).ok_or_else(|| ProtocolError::response(INVALID_FLOAT))?;
        let value = (old + increment).value;
        if !value.is_finite() {
            return Err(ProtocolError::response(NON_FINITE_RESULT));
        }
        Ok(float::format(value))
    }
}

impl ComputeCommand for IncrByFloatReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::IncrByFloat(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        // Redis checks the key type and existing value before the increment.
        let old = match &entry.value.data {
            ValueObject::Int(value) => X87DoubleExtended::from_i128(*value as i128).value,
            ValueObject::String(bytes) => match float::parse(bytes) {
                Some(value) => value,
                None => {
                    return (
                        MochaOperation::Abort,
                        ProtocolError::response(INVALID_FLOAT).into(),
                    );
                }
            },
            _ => return (MochaOperation::Abort, ProtocolError::WrongType.into()),
        };
        let value = match self.incremented_value(old) {
            Ok(value) => value,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(value.clone())),
                expire: entry.get_expire_policy(),
            },
            Value::BulkString(Some(value)),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let value = match self.incremented_value(X87DoubleExtended::ZERO) {
            Ok(value) => value,
            Err(error) => return (MochaOperation::Abort, error.into()),
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(value.clone())),
                expire: ExpirePolicy::Persistent,
            },
            // INCRBYFLOAT returns a string even under RESP3.
            Value::BulkString(Some(value)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn request(increment: &[u8]) -> IncrByFloatReq {
        IncrByFloatReq {
            key: "key".into(),
            increment: Bytes::copy_from_slice(increment),
        }
    }

    fn entry(data: ValueObject) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(data),
            expire_at: Some(10_000),
        }
    }

    fn assert_success(
        result: (MochaOperation<MyValue>, Value),
        expected: &str,
        expires_at: Option<u64>,
    ) {
        let (MochaOperation::Insert { value, expire }, reply) = result else {
            panic!("expected successful insertion");
        };
        match (expire, expires_at) {
            (ExpirePolicy::Persistent, None) => {}
            (ExpirePolicy::Absolute(actual), Some(expected)) => assert_eq!(actual, expected),
            other => panic!("unexpected expiration: {other:?}"),
        }
        let ValueObject::String(stored) = value.data else {
            panic!("INCRBYFLOAT must store a string");
        };
        assert_eq!(stored.as_ref(), expected.as_bytes());
        let wire = format!("${}\r\n{expected}\r\n", expected.len());
        for proto in [2, 3] {
            assert_eq!(reply.encode_proto(proto), wire.as_bytes());
        }
    }

    fn assert_error(result: (MochaOperation<MyValue>, Value), expected: &str) {
        assert!(matches!(result.0, MochaOperation::Abort));
        assert_eq!(result.1.encode(), format!("-{expected}\r\n").as_bytes());
    }

    #[test]
    fn incrbyfloat_validates_arity_and_preserves_binary_keys_in_raft_requests() {
        for count in [0, 1, 2, 4] {
            let args = vec![Value::BulkString(Some("arg".into())); count];
            assert!(matches!(
                IncrByFloatCommand.raft_request(&args),
                Err(ProtocolError::WrongArgCount("incrbyfloat"))
            ));
        }
        let args = [
            Value::BulkString(Some("INCRBYFLOAT".into())),
            Value::BulkString(Some(Bytes::from_static(b"\xff\0key"))),
            Value::BulkString(Some(".125".into())),
        ];
        let operation = IncrByFloatCommand.raft_request(&args).unwrap();
        let encoded = bincode2::serialize(&operation).unwrap();
        let Operation::Base(BaseOperation::IncrByFloat(request)) =
            bincode2::deserialize(&encoded).unwrap()
        else {
            panic!("expected INCRBYFLOAT request");
        };
        assert_eq!(request.key.as_ref(), b"\xff\0key");
        assert_success(request.init(), "0.125", None);
    }

    #[test]
    fn incrbyfloat_initializes_missing_keys_and_normalizes_zero() {
        for (increment, expected) in [
            ("0", "0"),
            ("-0", "0"),
            ("1e-18", "0"),
            ("-1e-18", "0"),
            ("+01.50", "1.5"),
            ("-0x1.8p1", "-3"),
            ("1e20", "100000000000000000000"),
        ] {
            assert_success(request(increment.as_bytes()).init(), expected, None);
        }
    }

    #[test]
    fn incrbyfloat_preserves_precision_and_ttl_for_string_values() {
        for (old, increment, expected) in [
            ("10.50", "0.1", "10.6"),
            ("0.1", "0.2", "0.3"),
            ("5e3", "2e2", "5200"),
            ("10.5", "-10.5", "0"),
            ("-0", "-0", "0"),
            ("+01.50", "0", "1.5"),
            ("1", "0.00000000000000001", "1.00000000000000001"),
            ("9007199254740993", "1", "9007199254740994"),
            ("0x1.8p1", "0X1P-1", "3.5"),
        ] {
            let stored = entry(ValueObject::String(Bytes::copy_from_slice(old.as_bytes())));
            assert_success(
                request(increment.as_bytes()).mutate(stored, 0),
                expected,
                Some(10_000),
            );
        }
    }

    #[test]
    fn incrbyfloat_converts_integer_encoding_without_narrowing_to_f64() {
        for (old, increment, expected) in [
            (9_007_199_254_740_993, "1", "9007199254740994"),
            (i64::MAX, "1", "9223372036854775808"),
            (i64::MIN, "-1", "-9223372036854775809"),
            (42, ".5", "42.5"),
        ] {
            assert_success(
                request(increment.as_bytes()).mutate(entry(ValueObject::Int(old)), 0),
                expected,
                Some(10_000),
            );
        }
    }

    #[test]
    fn incrbyfloat_rejects_invalid_numbers_without_writing() {
        for invalid in [
            b"".as_slice(),
            b" 1",
            b"1 ",
            b"+",
            b"1e+",
            b"1\0",
            b"\xff",
            b"NaN",
            b"nan(payload)",
            b"1e4933",
            b"1e-5000",
        ] {
            assert_error(request(invalid).init(), INVALID_FLOAT);
            assert_error(
                request(invalid).mutate(entry(ValueObject::Int(42)), 0),
                INVALID_FLOAT,
            );
            assert_error(
                request(b"1").mutate(
                    entry(ValueObject::String(Bytes::copy_from_slice(invalid))),
                    0,
                ),
                INVALID_FLOAT,
            );
        }
    }

    #[test]
    fn incrbyfloat_rejects_non_finite_results_without_writing() {
        for increment in [b"inf".as_slice(), b"-inf", b"+Infinity"] {
            assert_error(request(increment).init(), NON_FINITE_RESULT);
        }
        for (old, increment) in [
            ("inf", "1"),
            ("inf", "-inf"),
            ("-inf", "-1"),
            ("1e4932", "1e4932"),
            ("-1e4932", "-1e4932"),
        ] {
            assert_error(
                request(increment.as_bytes()).mutate(
                    entry(ValueObject::String(Bytes::copy_from_slice(old.as_bytes()))),
                    0,
                ),
                NON_FINITE_RESULT,
            );
        }
    }

    #[test]
    fn incrbyfloat_checks_key_type_before_the_increment() {
        let wrong_type = entry(ValueObject::Hash(Arc::new(Mutex::new(Default::default()))));
        for increment in [b"1".as_slice(), b"invalid", b"inf"] {
            assert_error(
                request(increment).mutate(wrong_type.clone(), 0),
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
        // An invalid old string also takes precedence over an infinite sum.
        assert_error(
            request(b"inf").mutate(entry(ValueObject::String("invalid".into())), 0),
            INVALID_FLOAT,
        );
    }
}
