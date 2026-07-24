use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BitPosUnit {
    Byte,
    Bit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitPosParams {
    pub key: Bytes,

    /// Redis 只允许 0 或 1。
    pub bit: u8,

    /// BYTE 模式下是字节下标，BIT 模式下是位下标。
    pub start: Option<i64>,

    /// 是否为 Some 同时决定了是否启用 Redis 的“右侧补零”语义。
    pub end: Option<i64>,

    pub unit: BitPosUnit,
}

impl Display for BitPosParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BitPosParams {{ key: {:?}, bit: {}, start: {:?}, end: {:?}, unit: {:?} }}",
            self.key, self.bit, self.start, self.end, self.unit
        )
    }
}

impl BitPosParams {
    fn execute_bytes(&self, bytes: &[u8]) -> Value {
        /*
         * Redis 对空字符串有特殊处理：
         *
         * BITPOS missing-key 0 -> 0
         * BITPOS missing-key 1 -> -1
         *
         * 即使显式给出 0 -1 BIT，查找 0 仍然返回 0。
         */
        if bytes.is_empty() {
            return Value::Integer(if self.bit == 0 { 0 } else { -1 });
        }

        let unit_len = match self.unit {
            BitPosUnit::Byte => bytes.len(),
            BitPosUnit::Bit => match bytes.len().checked_mul(8) {
                Some(len) => len,
                None => {
                    return ProtocolError::Custom(
                        "ERR string exceeds maximum allowed size",
                    )
                        .into();
                }
            },
        };

        let Some((start_unit, end_unit)) =
            normalize_range(unit_len, self.start, self.end)
        else {
            return Value::Integer(-1);
        };

        let (start_bit, end_bit) = match self.unit {
            BitPosUnit::Byte => {
                let start_bit = match start_unit.checked_mul(8) {
                    Some(value) => value,
                    None => {
                        return ProtocolError::Custom(
                            "ERR bit position is out of range",
                        )
                            .into();
                    }
                };

                let end_bit = match end_unit
                    .checked_mul(8)
                    .and_then(|value| value.checked_add(7))
                {
                    Some(value) => value,
                    None => {
                        return ProtocolError::Custom(
                            "ERR bit position is out of range",
                        )
                            .into();
                    }
                };

                (start_bit, end_bit)
            }
            BitPosUnit::Bit => (start_unit, end_unit),
        };

        if let Some(position) = find_first_bit(bytes, self.bit, start_bit, end_bit) {
            return usize_to_integer_value(position);
        }

        /*
         * Redis 的特殊语义：
         *
         * 没有显式指定 end，并且查找的是 0 时，
         * 字符串右边被认为无限补 0。
         *
         * 因此全为 0xff 的三字节字符串：
         *
         * BITPOS key 0       -> 24
         * BITPOS key 0 0     -> 24
         * BITPOS key 0 0 -1  -> -1
         */
        if self.bit == 0 && self.end.is_none() {
            return match bytes.len().checked_mul(8) {
                Some(position) => usize_to_integer_value(position),
                None => ProtocolError::Custom("ERR bit position is out of range").into(),
            };
        }

        Value::Integer(-1)
    }
}

impl ReadCommand for BitPosParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let bytes: Vec<u8> = match value {
            None => Vec::new(),
            Some(value) => match value.value.data {
                ValueObject::String(value) => value.to_vec(),

                // Redis 的整数编码字符串按十进制字符串内容执行位操作。
                ValueObject::Int(value) => value.to_string().into_bytes(),

                _ => return ProtocolError::WrongType.into(),
            },
        };

        self.execute_bytes(&bytes)
    }
}

/// 按 Redis/GETRANGE 的方式规范化闭区间。
///
/// 例如长度为 3：
///
/// - start = -1 -> 2
/// - start = -2 -> 1
/// - end = 100  -> 2
/// - start 超过结尾或 start > end -> 空范围
fn normalize_range(
    len: usize,
    start: Option<i64>,
    end: Option<i64>,
) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len = len as i128;

    let mut start = i128::from(start.unwrap_or(0));
    let mut end = end.map(i128::from).unwrap_or(len - 1);

    if start < 0 {
        start += len;
    }

    if end < 0 {
        end += len;
    }

    if start < 0 {
        start = 0;
    }

    if end < 0 || start >= len || start > end {
        return None;
    }

    if end >= len {
        end = len - 1;
    }

    Some((start as usize, end as usize))
}

/// 在闭区间 `[start_bit, end_bit]` 中搜索第一个目标位。
///
/// Redis 的位顺序是：
///
/// - 每个字节的最高位是较小的 offset；
/// - 第一个字节依次对应 offset 0..7。
///
/// 对完整字节使用 leading_zeros 快速定位；范围边缘按位处理。
fn find_first_bit(
    bytes: &[u8],
    target_bit: u8,
    start_bit: usize,
    end_bit: usize,
) -> Option<usize> {
    debug_assert!(target_bit <= 1);
    debug_assert!(start_bit <= end_bit);
    debug_assert!(end_bit < bytes.len() * 8);

    let mut position = start_bit;

    while position <= end_bit {
        let byte_index = position / 8;
        let bit_offset = position % 8;

        /*
         * 当前正好位于字节边界，并且整个字节都在搜索范围内时，
         * 一次处理完整的 8 位。
         */
        if bit_offset == 0 && end_bit - position >= 7 {
            let byte = bytes[byte_index];

            /*
             * 查找 1：candidate = byte
             * 查找 0：candidate = !byte
             *
             * candidate 中的 1 表示匹配目标位的位置。
             */
            let candidate = if target_bit == 1 { byte } else { !byte };

            if candidate != 0 {
                let offset = candidate.leading_zeros() as usize;
                return Some(position + offset);
            }

            position += 8;
            continue;
        }

        let byte = bytes[byte_index];
        let current_bit = (byte >> (7 - bit_offset)) & 1;

        if current_bit == target_bit {
            return Some(position);
        }

        position += 1;
    }

    None
}

fn usize_to_integer_value(value: usize) -> Value {
    match i64::try_from(value) {
        Ok(value) => Value::Integer(value),
        Err(_) => ProtocolError::Custom("ERR bit position is out of range").into(),
    }
}

pub struct BitPosCommand;

impl BitPosCommand {
    fn parse_args(items: &[Value]) -> Result<BitPosParams, ProtocolError> {
        if !(3..=6).contains(&items.len()) {
            return Err(ProtocolError::WrongArgCount("bitpos"));
        }

        /*
         * Redis 会优先检查第六个参数是否为 BYTE/BIT。
         *
         * 例如：
         * BITPOS key 0 0 1 hello
         * 应返回 syntax error，而不是整数解析错误。
         */
        let unit = if items.len() == 6 {
            Self::parse_unit(&items[5])?
        } else {
            BitPosUnit::Byte
        };

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("bitpos"))?;

        let bit = parse_redis_i64(&items[2]).ok_or(ProtocolError::Custom(
            "ERR value is not an integer or out of range",
        ))?;

        if bit != 0 && bit != 1 {
            return Err(ProtocolError::Custom(
                "ERR The bit argument must be 1 or 0",
            ));
        }

        let start = if items.len() >= 4 {
            Some(parse_redis_i64(&items[3]).ok_or(ProtocolError::Custom(
                "ERR value is not an integer or out of range",
            ))?)
        } else {
            None
        };

        let end = if items.len() >= 5 {
            Some(parse_redis_i64(&items[4]).ok_or(ProtocolError::Custom(
                "ERR value is not an integer or out of range",
            ))?)
        } else {
            None
        };

        Ok(BitPosParams {
            key,
            bit: bit as u8,
            start,
            end,
            unit,
        })
    }

    fn parse_unit(value: &Value) -> Result<BitPosUnit, ProtocolError> {
        let bytes = value
            .string_bytes_clone()
            .ok_or(ProtocolError::Custom("ERR syntax error"))?;

        if bytes.as_ref().eq_ignore_ascii_case(b"BYTE") {
            Ok(BitPosUnit::Byte)
        } else if bytes.as_ref().eq_ignore_ascii_case(b"BIT") {
            Ok(BitPosUnit::Bit)
        } else {
            Err(ProtocolError::Custom("ERR syntax error"))
        }
    }
}

/// 模拟 Redis string2ll 的严格整数解析规则：
///
/// - 不允许空字符串；
/// - 不允许 `+1`；
/// - 不允许 `01`、`-0`；
/// - 不允许空格或其他字符；
/// - 检查 i64 溢出。
fn parse_redis_i64(value: &Value) -> Option<i64> {
    let bytes = value.string_bytes_clone()?;
    parse_redis_i64_bytes(bytes.as_ref())
}

fn parse_redis_i64_bytes(bytes: &[u8]) -> Option<i64> {
    if bytes == b"0" {
        return Some(0);
    }

    if bytes.is_empty() {
        return None;
    }

    let (negative, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };

    // Redis 不接受空数字、前导 0、+ 前缀以及 -0。
    if digits.is_empty() || !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }

    let mut magnitude = 0_u64;

    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }

        magnitude = magnitude
            .checked_mul(10)?
            .checked_add(u64::from(digit - b'0'))?;
    }

    if negative {
        let min_magnitude = (i64::MAX as u64) + 1;

        if magnitude > min_magnitude {
            return None;
        }

        if magnitude == min_magnitude {
            Some(i64::MIN)
        } else {
            Some(-(magnitude as i64))
        }
    } else {
        if magnitude > i64::MAX as u64 {
            return None;
        }

        Some(magnitude as i64)
    }
}

impl ReadRaftCommand for BitPosCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BitPos(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for BitPosCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("BITPOS")));
        }

        let operation = self.read_operation(items)?;
        server.app.read(operation, client.db_number).await
    }
}