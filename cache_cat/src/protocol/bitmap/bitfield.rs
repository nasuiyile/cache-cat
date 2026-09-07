use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::BitField;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/*
 * Redis 默认字符串上限是 512 MiB。
 *
 * 最好把这里替换成 CacheCat 自己统一的最大字符串长度配置，
 * 让 SETBIT、GETBIT、BITFIELD、SETRANGE 等命令共享同一个限制。
 */
const MAX_BITFIELD_STRING_BYTES: u64 = 512 * 1024 * 1024;

const ERR_SYNTAX: &str = "ERR syntax error";
const ERR_INVALID_TYPE: &str = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.";
const ERR_INVALID_OFFSET: &str = "ERR bit offset is not an integer or out of range";
const ERR_INVALID_INTEGER: &str = "ERR value is not an integer or out of range";
const ERR_INVALID_OVERFLOW: &str = "ERR Invalid OVERFLOW type specified";

/// BITFIELD 的整数编码。
///
/// Redis 支持：
///
/// - i1 到 i64
/// - u1 到 u63
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BitFieldEncoding {
    pub signed: bool,
    pub bits: u8,
}

impl BitFieldEncoding {
    #[inline]
    pub fn signed(bits: u8) -> Self {
        Self { signed: true, bits }
    }

    #[inline]
    pub fn unsigned(bits: u8) -> Self {
        Self {
            signed: false,
            bits,
        }
    }

    #[inline]
    pub fn width(self) -> u64 {
        self.bits as u64
    }

    /// 当前编码能够表示的最小值和最大值。
    fn bounds(self) -> (i128, i128) {
        let bits = self.bits as u32;

        if self.signed {
            let sign_value = 1_i128 << (bits - 1);
            (-sign_value, sign_value - 1)
        } else {
            (0, (1_i128 << bits) - 1)
        }
    }

    /// 将原始位模式解释为当前编码的整数。
    fn decode(self, raw: u64) -> i64 {
        if !self.signed {
            // unsigned 最大只有 63 位，因此一定能够放入 i64。
            return raw as i64;
        }

        if self.bits == 64 {
            return raw as i64;
        }

        let bits = self.bits as u32;
        let sign_bit = 1_u64 << (bits - 1);

        if raw & sign_bit != 0 {
            // 符号扩展。
            let extended = raw | (!0_u64 << bits);
            extended as i64
        } else {
            raw as i64
        }
    }

    /// 将整数转换为当前编码需要写入的低 bits 位。
    fn encode(self, value: i64) -> u64 {
        let raw = value as u64;

        if self.bits == 64 {
            raw
        } else {
            raw & ((1_u64 << self.bits) - 1)
        }
    }

    /// 根据溢出策略，将一个数学整数规范化为当前编码可表示的值。
    fn normalize(self, value: i128, overflow: BitFieldOverflow) -> Option<i64> {
        let (min, max) = self.bounds();

        if value >= min && value <= max {
            return Some(value as i64);
        }

        match overflow {
            BitFieldOverflow::Fail => None,

            BitFieldOverflow::Sat => {
                if value < min {
                    Some(min as i64)
                } else {
                    Some(max as i64)
                }
            }

            BitFieldOverflow::Wrap => {
                let bits = self.bits as u32;
                let modulus = 1_i128 << bits;
                let mut wrapped = value.rem_euclid(modulus);

                if self.signed {
                    let sign_bit = 1_i128 << (bits - 1);

                    if wrapped >= sign_bit {
                        wrapped -= modulus;
                    }
                }

                Some(wrapped as i64)
            }
        }
    }
}

impl Display for BitFieldEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = if self.signed { 'i' } else { 'u' };
        write!(f, "{}{}", prefix, self.bits)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BitFieldOverflow {
    Wrap,
    Sat,
    Fail,
}

impl Default for BitFieldOverflow {
    fn default() -> Self {
        Self::Wrap
    }
}

impl Display for BitFieldOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitFieldOverflow::Wrap => write!(f, "WRAP"),
            BitFieldOverflow::Sat => write!(f, "SAT"),
            BitFieldOverflow::Fail => write!(f, "FAIL"),
        }
    }
}

/// OVERFLOW 本身不产生返回值。
///
/// 解析时会把当前 OVERFLOW 策略复制到后续 SET/INCRBY 子命令中，
/// 从而保证 Raft 日志包含完整、确定的执行语义。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BitFieldSubCommand {
    Get {
        encoding: BitFieldEncoding,
        offset: u64,
    },

    Set {
        encoding: BitFieldEncoding,
        offset: u64,
        value: i64,
        overflow: BitFieldOverflow,
    },

    IncrBy {
        encoding: BitFieldEncoding,
        offset: u64,
        increment: i64,
        overflow: BitFieldOverflow,
    },
}

impl BitFieldSubCommand {
    #[inline]
    fn is_write(self) -> bool {
        matches!(
            self,
            BitFieldSubCommand::Set { .. } | BitFieldSubCommand::IncrBy { .. }
        )
    }

    fn end_bit(self) -> Option<u64> {
        let (encoding, offset) = match self {
            BitFieldSubCommand::Get { encoding, offset }
            | BitFieldSubCommand::Set {
                encoding, offset, ..
            }
            | BitFieldSubCommand::IncrBy {
                encoding, offset, ..
            } => (encoding, offset),
        };

        offset.checked_add(encoding.width() - 1)
    }
}

impl Display for BitFieldSubCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitFieldSubCommand::Get { encoding, offset } => {
                write!(f, "GET {} {}", encoding, offset)
            }

            BitFieldSubCommand::Set {
                encoding,
                offset,
                value,
                overflow,
            } => {
                write!(
                    f,
                    "SET {} {} {} OVERFLOW {}",
                    encoding, offset, value, overflow
                )
            }

            BitFieldSubCommand::IncrBy {
                encoding,
                offset,
                increment,
                overflow,
            } => {
                write!(
                    f,
                    "INCRBY {} {} {} OVERFLOW {}",
                    encoding, offset, increment, overflow
                )
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitFieldParams {
    pub key: Bytes,
    pub operations: Vec<BitFieldSubCommand>,
}

impl Display for BitFieldParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BitFieldParams {{ key: {}, operations: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.operations
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitFieldReq {
    pub key: Bytes,
    pub operations: Vec<BitFieldSubCommand>,
}

impl Display for BitFieldReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BitFieldReq {{ key: {}, operations: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.operations
        )
    }
}

impl BitFieldReq {
    #[inline]
    fn has_write_operation(&self) -> bool {
        self.operations.iter().any(|operation| operation.is_write())
    }

    /// Redis 会根据写子命令访问到的最远位扩展字符串。
    fn highest_write_end_bit(&self) -> Option<u64> {
        self.operations
            .iter()
            .copied()
            .filter(|operation| operation.is_write())
            .filter_map(BitFieldSubCommand::end_bit)
            .max()
    }

    fn prepare_write_buffer(&self, bytes: &mut BytesMut) {
        let Some(end_bit) = self.highest_write_end_bit() else {
            return;
        };

        let required_len_u64 = (end_bit >> 3) + 1;

        // parse_offset 已经验证过长度限制。
        let required_len =
            usize::try_from(required_len_u64).expect("BITFIELD required length must fit in usize");

        if bytes.len() < required_len {
            bytes.resize(required_len, 0);
        }
    }

    fn execute_operations(&self, bytes: &mut BytesMut) -> Vec<Value> {
        /*
         * Redis 在开始执行子命令前，会按照最远的写偏移扩展字符串。
         *
         * GET-only 命令不会扩展字符串。
         */
        self.prepare_write_buffer(bytes);

        let mut replies = Vec::with_capacity(self.operations.len());

        for operation in self.operations.iter().copied() {
            match operation {
                BitFieldSubCommand::Get { encoding, offset } => {
                    let current = read_bitfield(bytes, encoding, offset);
                    replies.push(Value::Integer(current));
                }

                BitFieldSubCommand::Set {
                    encoding,
                    offset,
                    value,
                    overflow,
                } => {
                    let old_value = read_bitfield(bytes, encoding, offset);
                    let normalized = encoding.normalize(value as i128, overflow);

                    match normalized {
                        Some(new_value) => {
                            write_bitfield(bytes, encoding, offset, new_value);

                            // SET 返回写入前的旧值。
                            replies.push(Value::Integer(old_value));
                        }

                        None => {
                            // OVERFLOW FAIL：不修改对应字段，返回 nil。
                            replies.push(null_value());
                        }
                    }
                }

                BitFieldSubCommand::IncrBy {
                    encoding,
                    offset,
                    increment,
                    overflow,
                } => {
                    let old_value = read_bitfield(bytes, encoding, offset);

                    /*
                     * 使用 i128 计算，避免：
                     *
                     * - i64 + i64 在 Rust 中溢出；
                     * - i64 编码自身溢出；
                     * - u63 加负数时出现类型转换问题。
                     */
                    let mathematical_result = old_value as i128 + increment as i128;
                    let normalized = encoding.normalize(mathematical_result, overflow);

                    match normalized {
                        Some(new_value) => {
                            write_bitfield(bytes, encoding, offset, new_value);

                            // INCRBY 返回增加后的新值。
                            replies.push(Value::Integer(new_value));
                        }

                        None => {
                            // OVERFLOW FAIL：不修改对应字段，返回 nil。
                            replies.push(null_value());
                        }
                    }
                }
            }
        }

        replies
    }
}

/// 从字符串中读取一个位字段。
///
/// 字符串之外的位按 0 处理。
fn read_bitfield(bytes: &[u8], encoding: BitFieldEncoding, offset: u64) -> i64 {
    let mut raw = 0_u64;

    for bit_index in 0..encoding.width() {
        let absolute_bit = offset + bit_index;
        let byte_index = (absolute_bit >> 3) as usize;
        let bit_position = 7 - (absolute_bit & 7);

        let bit = if byte_index < bytes.len() {
            (bytes[byte_index] >> bit_position) & 1
        } else {
            0
        };

        raw = (raw << 1) | bit as u64;
    }

    encoding.decode(raw)
}

/// 将一个整数写入字符串中的指定字段。
///
/// 调用前必须保证 bytes 已经扩展到足够长度。
fn write_bitfield(bytes: &mut BytesMut, encoding: BitFieldEncoding, offset: u64, value: i64) {
    let raw = encoding.encode(value);

    for bit_index in 0..encoding.width() {
        let absolute_bit = offset + bit_index;
        let byte_index = (absolute_bit >> 3) as usize;
        let bit_position = 7 - (absolute_bit & 7);

        let source_shift = encoding.width() - 1 - bit_index;
        let bit = ((raw >> source_shift) & 1) as u8;
        let mask = 1_u8 << bit_position;

        debug_assert!(byte_index < bytes.len());

        if bit == 1 {
            bytes[byte_index] |= mask;
        } else {
            bytes[byte_index] &= !mask;
        }
    }
}

/// 假设 response_value::Value 中的 RESP Null 变体叫做 `Null`。
/// 如果项目中叫做 Nil、NullBulkString 等，只需要改这一行。
#[inline]
fn null_value() -> Value {
    Value::Array(None)
}

impl ComputeCommand for BitFieldReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BitField(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();

        /*
         * Redis 的位操作基于 String。
         *
         * CacheCat 的 Int 看起来是 String 的整数编码形式，因此和
         * SETBIT 一样，先转换为十进制字符串字节。
         */
        let source = match &entry.value.data {
            ValueObject::String(data) => data.clone(),

            ValueObject::Int(value) => value.to_string().into(),

            _ => {
                return (MochaOperation::Abort, ProtocolError::WrongType.into());
            }
        };

        let mut bytes = BytesMut::from(source);
        let has_write = self.has_write_operation();
        let replies = self.execute_operations(&mut bytes);

        let operation = if has_write {
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(bytes.freeze())),
                expire,
            }
        } else {
            /*
             * BITFIELD key GET ... 或 BITFIELD key 没有实际子命令时，
             * 不能修改字符串，也不能改变 TTL。
             */
            MochaOperation::Abort
        };

        (operation, Value::Array(Some(replies)))
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut bytes = BytesMut::new();
        let has_write = self.has_write_operation();
        let replies = self.execute_operations(&mut bytes);

        let operation = if has_write {
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(bytes.freeze())),
                expire: ExpirePolicy::Persistent,
            }
        } else {
            /*
             * 不存在的 key 执行 GET 时只返回 0，不创建 key。
             */
            MochaOperation::Abort
        };

        (operation, Value::Array(Some(replies)))
    }
}

pub struct BitFieldCommand;

impl BitFieldCommand {
    fn parse_args(items: &[Value]) -> Result<BitFieldParams, ProtocolError> {
        /*
         * BITFIELD key 是合法的，返回空数组。
         * 因此这里只要求命令名和 key。
         */
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("bitfield"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("bitfield"))?;

        let mut operations = Vec::new();
        let mut overflow = BitFieldOverflow::Wrap;
        let mut index = 2;

        while index < items.len() {
            let subcommand = parse_upper_token(&items[index])?;

            match subcommand.as_str() {
                "GET" => {
                    if index + 2 >= items.len() {
                        return Err(ProtocolError::response(ERR_SYNTAX));
                    }

                    let encoding = parse_encoding(&items[index + 1])?;
                    let offset = parse_offset(&items[index + 2], encoding)?;

                    operations.push(BitFieldSubCommand::Get { encoding, offset });
                    index += 3;
                }

                "SET" => {
                    if index + 3 >= items.len() {
                        return Err(ProtocolError::response(ERR_SYNTAX));
                    }

                    let encoding = parse_encoding(&items[index + 1])?;
                    let offset = parse_offset(&items[index + 2], encoding)?;
                    let value = parse_i64_argument(&items[index + 3])?;

                    operations.push(BitFieldSubCommand::Set {
                        encoding,
                        offset,
                        value,
                        overflow,
                    });

                    index += 4;
                }

                "INCRBY" => {
                    if index + 3 >= items.len() {
                        return Err(ProtocolError::response(ERR_SYNTAX));
                    }

                    let encoding = parse_encoding(&items[index + 1])?;
                    let offset = parse_offset(&items[index + 2], encoding)?;
                    let increment = parse_i64_argument(&items[index + 3])?;

                    operations.push(BitFieldSubCommand::IncrBy {
                        encoding,
                        offset,
                        increment,
                        overflow,
                    });

                    index += 4;
                }

                "OVERFLOW" => {
                    if index + 1 >= items.len() {
                        return Err(ProtocolError::response(ERR_SYNTAX));
                    }

                    overflow = parse_overflow(&items[index + 1])?;
                    index += 2;
                }

                _ => {
                    return Err(ProtocolError::response(ERR_SYNTAX));
                }
            }
        }

        Ok(BitFieldParams { key, operations })
    }
}

fn parse_upper_token(value: &Value) -> Result<String, ProtocolError> {
    let bytes = value
        .string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("bitfield"))?;

    let string = std::str::from_utf8(&bytes).map_err(|_| ProtocolError::response(ERR_SYNTAX))?;

    Ok(string.to_ascii_uppercase())
}

fn parse_encoding(value: &Value) -> Result<BitFieldEncoding, ProtocolError> {
    let bytes = value
        .string_bytes_clone()
        .ok_or(ProtocolError::response(ERR_INVALID_TYPE))?;

    if bytes.len() < 2 {
        return Err(ProtocolError::response(ERR_INVALID_TYPE));
    }

    /*
     * Redis 的 encoding 前缀是小写 i/u。
     * 例如 i8、u16。
     */
    let signed = match bytes[0] {
        b'i' => true,
        b'u' => false,
        _ => return Err(ProtocolError::response(ERR_INVALID_TYPE)),
    };

    let width_string =
        std::str::from_utf8(&bytes[1..]).map_err(|_| ProtocolError::response(ERR_INVALID_TYPE))?;

    let bits = width_string
        .parse::<u8>()
        .map_err(|_| ProtocolError::response(ERR_INVALID_TYPE))?;

    let valid = if signed {
        (1..=64).contains(&bits)
    } else {
        (1..=63).contains(&bits)
    };

    if !valid {
        return Err(ProtocolError::response(ERR_INVALID_TYPE));
    }

    Ok(BitFieldEncoding { signed, bits })
}

fn parse_offset(value: &Value, encoding: BitFieldEncoding) -> Result<u64, ProtocolError> {
    let bytes = value
        .string_bytes_clone()
        .ok_or(ProtocolError::response(ERR_INVALID_OFFSET))?;

    let (multiply_by_width, number_bytes) = match bytes.first() {
        Some(b'#') => (true, &bytes[1..]),
        _ => (false, bytes.as_ref()),
    };

    if number_bytes.is_empty() {
        return Err(ProtocolError::response(ERR_INVALID_OFFSET));
    }

    let number_string = std::str::from_utf8(number_bytes)
        .map_err(|_| ProtocolError::response(ERR_INVALID_OFFSET))?;

    /*
     * 使用 u64 解析：
     *
     * - 自动拒绝负数；
     * - 自动拒绝超出 u64 的数字。
     */
    let base_offset = number_string
        .parse::<u64>()
        .map_err(|_| ProtocolError::response(ERR_INVALID_OFFSET))?;

    let offset = if multiply_by_width {
        base_offset
            .checked_mul(encoding.width())
            .ok_or(ProtocolError::response(ERR_INVALID_OFFSET))?
    } else {
        base_offset
    };

    let end_bit = offset
        .checked_add(encoding.width() - 1)
        .ok_or(ProtocolError::response(ERR_INVALID_OFFSET))?;

    let required_bytes = (end_bit >> 3)
        .checked_add(1)
        .ok_or(ProtocolError::response(ERR_INVALID_OFFSET))?;

    if required_bytes > MAX_BITFIELD_STRING_BYTES {
        return Err(ProtocolError::response(ERR_INVALID_OFFSET));
    }

    usize::try_from(required_bytes).map_err(|_| ProtocolError::response(ERR_INVALID_OFFSET))?;

    Ok(offset)
}

fn parse_i64_argument(value: &Value) -> Result<i64, ProtocolError> {
    let bytes = value
        .string_bytes_clone()
        .ok_or(ProtocolError::response(ERR_INVALID_INTEGER))?;

    let string =
        std::str::from_utf8(&bytes).map_err(|_| ProtocolError::response(ERR_INVALID_INTEGER))?;

    string
        .parse::<i64>()
        .map_err(|_| ProtocolError::response(ERR_INVALID_INTEGER))
}

fn parse_overflow(value: &Value) -> Result<BitFieldOverflow, ProtocolError> {
    let token = parse_upper_token(value)?;

    match token.as_str() {
        "WRAP" => Ok(BitFieldOverflow::Wrap),
        "SAT" => Ok(BitFieldOverflow::Sat),
        "FAIL" => Ok(BitFieldOverflow::Fail),
        _ => Err(ProtocolError::response(ERR_INVALID_OVERFLOW)),
    }
}

impl RaftCommand for BitFieldCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(BitField(BitFieldReq {
            key: params.key,
            operations: params.operations,
        })))
    }
}

#[async_trait]
impl Command for BitFieldCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
