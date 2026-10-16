use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BitCountUnit {
    Byte,
    Bit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitCountParams {
    pub key: Bytes,
    pub start: Option<i64>,
    pub end: Option<i64>,
    /// Redis 7.0+: BYTE is the default; BIT counts individual bits.
    pub unit: BitCountUnit,
}

impl Display for BitCountParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BitCountParams {{ key: {:?}, start: {:?}, end: {:?}, unit: {:?} }}",
            self.key, self.start, self.end, self.unit
        )
    }
}

impl ReadCommand for BitCountParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let Some(value) = value else {
            return Value::Integer(0);
        };
        let integer;
        let bytes = match &value.value.data {
            ValueObject::String(s) => s.as_ref(),
            ValueObject::Int(i) => {
                integer = i.to_string();
                integer.as_bytes()
            }
            _ => return ProtocolError::WrongType.into(),
        };

        let unit_len = match self.unit {
            BitCountUnit::Byte => bytes.len(),
            BitCountUnit::Bit => match bytes.len().checked_mul(8) {
                Some(len) => len,
                None => {
                    return ProtocolError::response("ERR string exceeds maximum allowed size")
                        .into();
                }
            },
        };
        if unit_len == 0 {
            return Value::Integer(0);
        }

        // If no range is specified, count all bits
        if self.start.is_none() && self.end.is_none() {
            let count: i64 = bytes.iter().map(|&byte| byte.count_ones() as i64).sum();
            return Value::Integer(count);
        }

        // Convert negative indices to positive
        let len = unit_len as i64;
        let start = self.start.unwrap_or(0);
        let end = self.end.unwrap_or(-1);

        // Preserve a reversed negative range before clipping both ends to zero.
        if start < 0 && end < 0 && start > end {
            return Value::Integer(0);
        }

        let start = if start < 0 { start + len } else { start };
        let end = if end < 0 { end + len } else { end };

        // Clamp indices to valid range
        let start = start.max(0);
        let end = end.max(0).min(len - 1);

        // If start > end after conversion, return 0
        if start > end {
            return Value::Integer(0);
        }

        // Count bits in the specified range
        let count: i64 = match self.unit {
            BitCountUnit::Byte => bytes[start as usize..=end as usize]
                .iter()
                .map(|&byte| byte.count_ones() as i64)
                .sum(),
            BitCountUnit::Bit => {
                let start = start as usize;
                let end = end as usize;
                let first = start / 8;
                let last = end / 8;
                let first_mask = u8::MAX >> (start % 8);
                let last_mask = u8::MAX << (7 - (end % 8));
                if first == last {
                    i64::from((bytes[first] & first_mask & last_mask).count_ones())
                } else {
                    i64::from((bytes[first] & first_mask).count_ones())
                        + bytes[first + 1..last]
                            .iter()
                            .map(|byte| i64::from(byte.count_ones()))
                            .sum::<i64>()
                        + i64::from((bytes[last] & last_mask).count_ones())
                }
            }
        };

        Value::Integer(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_requires_canonical_integers() {
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
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            for (start, end) in [(invalid, "-1"), ("0", invalid)] {
                let items = ["BITCOUNT", "key", start, end]
                    .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
                assert_eq!(
                    BitCountCommand.raft_request(&items).unwrap_err(),
                    ProtocolError::NotAnInteger,
                    "start {start:?}, end {end:?}"
                );
            }
        }
        for number in [
            "0",
            "1",
            "-1",
            "-9223372036854775808",
            "9223372036854775807",
        ] {
            let items = ["BITCOUNT", "key", number, number]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert!(BitCountCommand.raft_request(&items).is_ok(), "{number}");
        }
    }

    #[test]
    fn range_requires_both_start_and_end() {
        let items = ["BITCOUNT", "key", "0"]
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
        assert_eq!(
            BitCountCommand::parse_args(&items).unwrap_err(),
            ProtocolError::SyntaxError
        );
        for args in [vec!["BITCOUNT", "key"], vec!["BITCOUNT", "key", "0", "-1"]] {
            let items = args
                .into_iter()
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))))
                .collect::<Vec<_>>();
            assert!(BitCountCommand::parse_args(&items).is_ok());
        }
        let bit = ["BITCOUNT", "key", "0", "7", "BIT"]
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
        assert!(matches!(
            BitCountCommand::parse_args(&bit).unwrap().unit,
            BitCountUnit::Bit
        ));
        for unit in ["bits", "BYTE extra"] {
            let items = ["BITCOUNT", "key", "0", "7", unit]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert_eq!(
                BitCountCommand::parse_args(&items).unwrap_err(),
                ProtocolError::SyntaxError
            );
        }
    }

    #[test]
    fn unit_options_follow_redis_nul_termination() {
        for (unit, expected) in [
            ("bIt\0suffix", BitCountUnit::Bit),
            ("BYTE\0", BitCountUnit::Byte),
            ("bYtE\0suffix", BitCountUnit::Byte),
        ] {
            let items = ["BITCOUNT", "key", "0", "7", unit]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert_eq!(BitCountCommand::parse_args(&items).unwrap().unit, expected);
        }
        for unit in ["", "BI", "BI\0T", "BYT\0E", "BITS\0", "BYTEsuffix", "\0BIT"] {
            let items = ["BITCOUNT", "key", "0", "7", unit]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            assert_eq!(
                BitCountCommand::parse_args(&items).unwrap_err(),
                ProtocolError::SyntaxError
            );
        }
        for (name, expected) in [
            (b"BIT".as_slice(), BitCountUnit::Bit),
            (b"BYTE".as_slice(), BitCountUnit::Byte),
        ] {
            let mut option = vec![0xff; 1024 * 1024];
            option[..name.len()].copy_from_slice(name);
            option[name.len()] = 0;
            let mut items = ["BITCOUNT", "key", "0", "7", ""]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            items[4] = Value::BulkString(Some(Bytes::from(option)));
            assert_eq!(BitCountCommand::parse_args(&items).unwrap().unit, expected);
        }
    }

    #[test]
    fn bit_unit_counts_individual_bits_and_honors_negative_indexes() {
        let value = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(&[
                0b1000_0001,
                0b0000_0010,
            ]))),
            expire_at: None,
        };
        for (start, end, expected) in [
            ("0", "7", 2),
            ("7", "8", 1),
            ("0", "0", 1),
            ("1", "6", 0),
            ("7", "14", 2),
            ("-8", "-1", 1),
            ("0", "15", 3),
            ("16", "100", 0),
            ("9", "8", 0),
            ("-99", "-100", 0),
            ("-100", "-99", 1),
        ] {
            let items = ["BITCOUNT", "key", start, end, "bIt"]
                .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg.as_bytes()))));
            let params = BitCountCommand::parse_args(&items).unwrap();
            assert!(
                matches!(
                    params.execute(Some(value.clone())),
                    Value::Integer(actual) if actual == expected
                ),
                "range {start}..={end}"
            );
        }
    }

    #[test]
    fn integer_values_match_their_decimal_string_bytes() {
        for number in [0, 12345, i64::MIN] {
            let integer = EntrySnapshot {
                value: MyValue::new(ValueObject::Int(number)),
                expire_at: None,
            };
            let string = EntrySnapshot {
                value: MyValue::new(ValueObject::String(Bytes::from(number.to_string()))),
                expire_at: None,
            };
            for unit in [BitCountUnit::Byte, BitCountUnit::Bit] {
                for (start, end) in [
                    (None, None),
                    (Some(0), Some(-1)),
                    (Some(1), Some(3)),
                    (Some(-2), Some(-1)),
                ] {
                    let params = BitCountParams {
                        key: Bytes::from_static(b"key"),
                        start,
                        end,
                        unit,
                    };
                    match (
                        params.execute(Some(integer.clone())),
                        params.execute(Some(string.clone())),
                    ) {
                        (Value::Integer(actual), Value::Integer(expected)) => {
                            assert_eq!(actual, expected, "{number}, {unit:?}, {start:?}..={end:?}")
                        }
                        other => panic!("unexpected BITCOUNT results: {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn range_start_beyond_string_returns_zero() {
        let value = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"\xff"))),
            expire_at: None,
        };
        for start in [1, 100, i64::MAX] {
            let params = BitCountParams {
                key: Bytes::from_static(b"key"),
                start: Some(start),
                end: Some(i64::MAX),
                unit: BitCountUnit::Byte,
            };
            assert!(matches!(
                params.execute(Some(value.clone())),
                Value::Integer(0)
            ));
        }
        let params = BitCountParams {
            key: Bytes::from_static(b"key"),
            start: Some(-1),
            end: Some(-1),
            unit: BitCountUnit::Byte,
        };
        assert!(matches!(params.execute(Some(value)), Value::Integer(8)));
    }

    #[test]
    fn reversed_negative_range_remains_empty_after_clipping() {
        let value = EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"\xff"))),
            expire_at: None,
        };
        let mut params = BitCountParams {
            key: Bytes::from_static(b"key"),
            start: Some(-4),
            end: Some(-5),
            unit: BitCountUnit::Byte,
        };
        assert!(matches!(
            params.execute(Some(value.clone())),
            Value::Integer(0)
        ));
        params.start = Some(-5);
        params.end = Some(-4);
        assert!(matches!(params.execute(Some(value)), Value::Integer(8)));
    }
}

pub struct BitCountCommand;

impl BitCountCommand {
    fn parse_args(items: &[Value]) -> Result<BitCountParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("bitcount"));
        }
        if !matches!(items.len(), 2 | 4 | 5) {
            return Err(ProtocolError::SyntaxError);
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("bitcount"))?;

        let mut start = None;
        let mut end = None;

        if items.len() >= 3 {
            start = Some(items[2].try_parse_canonical_i64()?);
        }

        if items.len() >= 4 {
            end = Some(items[3].try_parse_canonical_i64()?);
        }

        let unit = if items.len() == 5 {
            let value = items[4]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&value, b"BYTE") {
                BitCountUnit::Byte
            } else if option_eq(&value, b"BIT") {
                BitCountUnit::Bit
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        } else {
            BitCountUnit::Byte
        };

        Ok(BitCountParams {
            key,
            start,
            end,
            unit,
        })
    }
}

impl ReadRaftCommand for BitCountCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BitCount(BitCountCommand::parse_args(items)?))
    }
}

#[async_trait]
impl Command for BitCountCommand {
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
        server.app.read(params, client.db_number).await
    }
}
