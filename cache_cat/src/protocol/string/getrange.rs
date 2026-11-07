use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetRangeParams {
    pub key: Bytes,
    pub start: i64,
    pub end: i64,
}

impl Display for GetRangeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GETRANGE {} {} {}",
            String::from_utf8_lossy(&self.key),
            self.start,
            self.end
        )
    }
}

impl GetRangeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("getrange"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let start = items[2].try_parse_canonical_i64()?;
        let end = items[3].try_parse_canonical_i64()?;
        Ok(Self { key, start, end })
    }

    fn slice(&self, bytes: Bytes) -> Bytes {
        // Redis checks reversed negative bounds before clamping. Otherwise,
        // e.g. -5..=-6 on a three-byte string would incorrectly return byte 0.
        if bytes.is_empty() || (self.start < 0 && self.end < 0 && self.start > self.end) {
            return Bytes::new();
        }

        // Rust byte slices cannot exceed isize::MAX. Adding a negative i64 to
        // this nonnegative length is safe even for i64::MIN.
        let len = bytes.len() as i64;
        let start = if self.start < 0 {
            (len + self.start).max(0)
        } else {
            self.start
        };
        let end = if self.end < 0 {
            (len + self.end).max(0)
        } else {
            self.end
        }
        .min(len - 1);

        if start > end {
            return Bytes::new();
        }
        // Share the immutable payload; a short range never copies the whole value.
        bytes.slice(start as usize..=end as usize)
    }
}

impl ReadCommand for GetRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let bytes = match value {
            None => return Value::BulkString(Some(Bytes::new())),
            Some(entry) => match entry.value.data {
                ValueObject::String(bytes) => bytes,
                ValueObject::Int(value) => Bytes::from(value.to_string()),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        Value::BulkString(Some(self.slice(bytes)))
    }
}

pub struct GetRangeCommand;

impl ReadRaftCommand for GetRangeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::GetRange(GetRangeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for GetRangeCommand {
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
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::entry::request::Operation;

    fn params(start: i64, end: i64) -> GetRangeParams {
        GetRangeParams {
            key: Bytes::from_static(b"key"),
            start,
            end,
        }
    }

    fn read(value: ValueObject, start: i64, end: i64) -> Value {
        params(start, end).execute(Some(EntrySnapshot {
            value: MyValue::new(value),
            expire_at: None,
        }))
    }

    fn assert_bulk(reply: Value, expected: &[u8]) {
        let Value::BulkString(Some(actual)) = reply else {
            panic!("GETRANGE must return a non-null bulk string: {reply:?}");
        };
        assert_eq!(actual.as_ref(), expected);
    }

    #[test]
    fn getrange_matches_redis_inclusive_and_negative_ranges() {
        let value = ValueObject::String(Bytes::from_static(b"This is a string"));
        for (start, end, expected) in [
            (0, 3, &b"This"[..]),
            (-3, -1, &b"ing"[..]),
            (0, -1, &b"This is a string"[..]),
            (10, 100, &b"string"[..]),
            (3, 3, &b"s"[..]),
            (16, 20, &b""[..]),
            (4, 3, &b""[..]),
            (-1, 0, &b""[..]),
            (-16, -16, &b"T"[..]),
            (0, -100, &b"T"[..]),
            (-100, -100, &b"T"[..]),
            (-100, -200, &b""[..]),
            (-200, -100, &b"T"[..]),
            (i64::MIN, i64::MAX, &b"This is a string"[..]),
            (i64::MIN, i64::MIN, &b"T"[..]),
            (i64::MIN + 1, i64::MIN, &b""[..]),
            (i64::MAX, i64::MAX, &b""[..]),
        ] {
            assert_bulk(read(value.clone(), start, end), expected);
        }
    }

    #[test]
    fn getrange_is_binary_safe_and_shares_the_string_payload() {
        let bytes = Bytes::from(Vec::from(&b"a\0\xff\xe4\xb8\xadb"[..]));
        let reply = read(ValueObject::String(bytes.clone()), 1, 4);
        let Value::BulkString(Some(range)) = reply else {
            panic!("expected bulk string");
        };
        assert_eq!(range.as_ref(), b"\0\xff\xe4\xb8");
        assert_eq!(range.as_ptr(), bytes[1..].as_ptr());
        for proto in [2, 3] {
            assert_eq!(
                Value::BulkString(Some(range.clone())).encode_proto(proto),
                b"$4\r\n\0\xff\xe4\xb8\r\n"
            );
        }
    }

    #[test]
    fn getrange_uses_decimal_bytes_for_integer_values() {
        for value in [0, 12345, -12345, i64::MIN, i64::MAX] {
            let decimal = value.to_string();
            assert_bulk(read(ValueObject::Int(value), 0, -1), decimal.as_bytes());
            assert_bulk(
                read(ValueObject::Int(value), -1, -1),
                &decimal.as_bytes()[decimal.len() - 1..],
            );
        }
        assert_bulk(read(ValueObject::Int(-12345), 0, 2), b"-12");
    }

    #[test]
    fn getrange_returns_empty_bulk_strings_and_checks_type_before_range() {
        for (start, end) in [(0, -1), (10, 0), (i64::MIN, i64::MAX)] {
            for reply in [
                params(start, end).execute(None),
                read(ValueObject::String(Bytes::new()), start, end),
            ] {
                for proto in [2, 3] {
                    assert_eq!(reply.encode_proto(proto), b"$0\r\n\r\n");
                }
            }
            let wrong_type = read(ValueObject::List(Default::default()), start, end);
            assert_eq!(
                wrong_type.encode(),
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            );
        }
    }

    #[test]
    fn getrange_parses_exact_arity_and_canonical_i64_bounds() {
        let args = |parts: &[&str]| {
            parts
                .iter()
                .map(|s| Value::BulkString(Some(Bytes::copy_from_slice(s.as_bytes()))))
                .collect::<Vec<_>>()
        };
        for count in [0, 1, 2, 3, 5] {
            let items = vec![Value::BulkString(Some("GETRANGE".into())); count];
            assert_eq!(
                GetRangeCommand.raft_request(&items).unwrap_err(),
                ProtocolError::WrongArgCount("getrange")
            );
        }
        for invalid in [
            "",
            "+1",
            "00",
            "01",
            "-0",
            "-01",
            " 1",
            "1 ",
            "1\0",
            "1.0",
            "no",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            for (start, end) in [(invalid, "-1"), ("0", invalid)] {
                assert_eq!(
                    GetRangeCommand
                        .raft_request(&args(&["GETRANGE", "key", start, end]))
                        .unwrap_err(),
                    ProtocolError::NotAnInteger
                );
            }
        }
        let operation = GetRangeCommand
            .raft_request(&args(&[
                "GETRANGE",
                "key\0",
                "-9223372036854775808",
                "9223372036854775807",
            ]))
            .unwrap();
        let Operation::Read(ReadOperation::GetRange(parsed)) = operation else {
            panic!("GETRANGE must be a replicated read operation");
        };
        assert_eq!(parsed.key.as_ref(), b"key\0");
        assert_eq!((parsed.start, parsed.end), (i64::MIN, i64::MAX));
    }
}
