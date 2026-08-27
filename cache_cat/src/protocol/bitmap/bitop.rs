use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::MultiReadComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// BITOP operation.
///
/// Redis <= 8.0:
/// - AND
/// - OR
/// - XOR
/// - NOT
///
/// Redis >= 8.2 additionally supports:
/// - DIFF
/// - DIFF1
/// - ANDOR
/// - ONE
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BitOp {
    And,
    Or,
    Xor,
    Not,
    Diff,
    Diff1,
    AndOr,
    One,
}

impl BitOp {
    fn parse(value: &[u8]) -> Result<Self, ProtocolError> {
        if value.eq_ignore_ascii_case(b"AND") {
            Ok(Self::And)
        } else if value.eq_ignore_ascii_case(b"OR") {
            Ok(Self::Or)
        } else if value.eq_ignore_ascii_case(b"XOR") {
            Ok(Self::Xor)
        } else if value.eq_ignore_ascii_case(b"NOT") {
            Ok(Self::Not)
        } else if value.eq_ignore_ascii_case(b"DIFF") {
            Ok(Self::Diff)
        } else if value.eq_ignore_ascii_case(b"DIFF1") {
            Ok(Self::Diff1)
        } else if value.eq_ignore_ascii_case(b"ANDOR") {
            Ok(Self::AndOr)
        } else if value.eq_ignore_ascii_case(b"ONE") {
            Ok(Self::One)
        } else {
            Err(ProtocolError::InvalidArgument("operation"))
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
            Self::Xor => "XOR",
            Self::Not => "NOT",
            Self::Diff => "DIFF",
            Self::Diff1 => "DIFF1",
            Self::AndOr => "ANDOR",
            Self::One => "ONE",
        }
    }
}

/// Parsed BITOP command parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BitOpParams {
    /// Destination key.
    pub key: Bytes,

    /// Source keys.
    pub keys: Vec<Bytes>,

    pub operation: BitOp,
}

/// BITOP command executor.
pub struct BitOpCommand;

impl BitOpCommand {
    fn parse(items: &[Value]) -> Result<BitOpParams, ProtocolError> {
        //
        // BITOP operation destkey key [key ...]
        //
        // Minimum:
        //
        // BITOP AND dest src
        //
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("BITOP"));
        }

        let operation_bytes = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("operation"))?;

        let operation = BitOp::parse(operation_bytes.as_ref())?;

        let key = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let keys = items
            .iter()
            .skip(3)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() != items.len() - 3 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        //
        // Redis semantics:
        //
        // BITOP NOT dest src
        //
        // NOT is unary and accepts exactly one source key.
        //
        if operation == BitOp::Not && keys.len() != 1 {
            return Err(ProtocolError::WrongArgCount("BITOP"));
        }

        Ok(BitOpParams {
            key,
            keys,
            operation,
        })
    }
}

impl RaftCommand for BitOpCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;

        Ok(Operation::Base(BaseOperation::BitOp(BitOpReq {
            key: params.key,
            keys: params.keys,
            operation: params.operation,
        })))
    }
}

#[async_trait]
impl Command for BitOpCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);

            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        let operation = self.raft_request(items)?;

        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BitOpReq {
    /// Destination key.
    pub key: Bytes,

    /// Source keys.
    pub keys: Vec<Bytes>,

    pub operation: BitOp,
}

impl Display for BitOpReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BITOP {} {}",
            self.operation.as_str(),
            String::from_utf8_lossy(&self.key),
        )?;

        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }

        Ok(())
    }
}

impl BitOpReq {
    /// Convert a Redis string-like ValueObject into its raw bytes.
    ///
    /// Important:
    ///
    /// ValueObject::Int must be treated as a Redis String, not as WRONGTYPE.
    ///
    /// Redis internally may integer-encode string values. BITOP operates on
    /// their decoded string representation.
    ///
    /// Examples:
    ///
    ///     SET foo 123
    ///
    /// BITOP must operate on:
    ///
    ///     b"123"
    ///
    /// rather than on the native binary representation of i64(123).
    fn value_to_bytes(value: &ValueObject) -> Result<Bytes, ProtocolError> {
        match value {
            ValueObject::String(value) => Ok(value.clone()),

            ValueObject::Int(value) => Ok(Bytes::from(value.to_string())),

            ValueObject::List(_)
            | ValueObject::Hash(_)
            | ValueObject::ZSet(_)
            | ValueObject::Set(_) => Err(ProtocolError::WrongType),
        }
    }

    /// Read one byte from a source.
    ///
    /// Redis BITOP semantics:
    ///
    /// Shorter strings are treated as zero-padded to the length of the
    /// longest input string.
    #[inline]
    fn byte_at(source: &[u8], index: usize) -> u8 {
        source.get(index).copied().unwrap_or(0)
    }

    fn compute(&self, sources: &[Bytes], max_len: usize) -> Vec<u8> {
        match self.operation {
            BitOp::And => Self::compute_and(sources, max_len),

            BitOp::Or => Self::compute_or(sources, max_len),

            BitOp::Xor => Self::compute_xor(sources, max_len),

            BitOp::Not => Self::compute_not(sources, max_len),

            BitOp::Diff => Self::compute_diff(sources, max_len),

            BitOp::Diff1 => Self::compute_diff1(sources, max_len),

            BitOp::AndOr => Self::compute_andor(sources, max_len),

            BitOp::One => Self::compute_one(sources, max_len),
        }
    }

    /// AND:
    ///
    /// dst[i] = src1[i] & src2[i] & ...
    ///
    /// A missing byte is zero, so if any source is shorter at this position,
    /// the result is zero.
    fn compute_and(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        let mut result = vec![0xff; max_len];

        for source in sources {
            for (index, output) in result.iter_mut().enumerate() {
                *output &= Self::byte_at(source, index);
            }
        }

        result
    }

    /// OR:
    ///
    /// dst[i] = src1[i] | src2[i] | ...
    fn compute_or(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        let mut result = vec![0; max_len];

        for source in sources {
            for (index, output) in result.iter_mut().enumerate() {
                *output |= Self::byte_at(source, index);
            }
        }

        result
    }

    /// XOR:
    ///
    /// dst[i] = src1[i] ^ src2[i] ^ ...
    fn compute_xor(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        let mut result = vec![0; max_len];

        for source in sources {
            for (index, output) in result.iter_mut().enumerate() {
                *output ^= Self::byte_at(source, index);
            }
        }

        result
    }

    /// NOT:
    ///
    /// dst[i] = !src[i]
    ///
    /// NOT is guaranteed by the parser to have exactly one source.
    fn compute_not(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        debug_assert_eq!(sources.len(), 1);

        let source = &sources[0];

        let mut result = Vec::with_capacity(max_len);

        for index in 0..max_len {
            result.push(!Self::byte_at(source, index));
        }

        result
    }

    /// Redis 8.2 DIFF:
    ///
    ///     X & !(Y1 | Y2 | ...)
    ///
    /// A bit is set when it exists in X but not in any Y.
    fn compute_diff(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        debug_assert!(!sources.is_empty());

        let x = &sources[0];

        let mut result = vec![0; max_len];

        for index in 0..max_len {
            let x_byte = Self::byte_at(x, index);

            let mut y_union = 0u8;

            for source in sources.iter().skip(1) {
                y_union |= Self::byte_at(source, index);
            }

            result[index] = x_byte & !y_union;
        }

        result
    }

    /// Redis 8.2 DIFF1:
    ///
    ///     (Y1 | Y2 | ...) & !X
    ///
    /// A bit is set when it exists in one of the Y inputs but not X.
    fn compute_diff1(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        debug_assert!(!sources.is_empty());

        let x = &sources[0];

        let mut result = vec![0; max_len];

        for index in 0..max_len {
            let x_byte = Self::byte_at(x, index);

            let mut y_union = 0u8;

            for source in sources.iter().skip(1) {
                y_union |= Self::byte_at(source, index);
            }

            result[index] = y_union & !x_byte;
        }

        result
    }

    /// Redis 8.2 ANDOR:
    ///
    ///     X & (Y1 | Y2 | ...)
    fn compute_andor(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        debug_assert!(!sources.is_empty());

        let x = &sources[0];

        let mut result = vec![0; max_len];

        for index in 0..max_len {
            let x_byte = Self::byte_at(x, index);

            let mut y_union = 0u8;

            for source in sources.iter().skip(1) {
                y_union |= Self::byte_at(source, index);
            }

            result[index] = x_byte & y_union;
        }

        result
    }

    /// Redis 8.2 ONE:
    ///
    /// A bit is set iff exactly one source has that bit set.
    ///
    /// We do not need a per-bit counter. For each byte:
    ///
    /// - `once` tracks bits seen an odd/current single number of times.
    /// - `multiple` tracks bits that have been seen at least twice.
    ///
    /// Exactly-one bits are:
    ///
    ///     once & !multiple
    fn compute_one(sources: &[Bytes], max_len: usize) -> Vec<u8> {
        let mut result = vec![0; max_len];

        for index in 0..max_len {
            let mut once = 0u8;
            let mut multiple = 0u8;

            for source in sources {
                let value = Self::byte_at(source, index);

                multiple |= once & value;
                once ^= value;
            }

            result[index] = once & !multiple;
        }

        result
    }
}

impl MultiReadComputeCommand for BitOpReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BitOp(self)
    }

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        //
        // First decode and validate ALL source keys.
        //
        // This is important:
        //
        // BITOP must return WRONGTYPE if any existing source key isn't a
        // Redis String.
        //
        // Missing keys are represented by an empty byte string here.
        // Later byte_at() automatically implements Redis' zero-padding rule.
        //
        let mut sources = Vec::with_capacity(read_entries.len());

        for entry in read_entries {
            let Some(snapshot) = entry else {
                //
                // Redis:
                //
                // A non-existent source key behaves like an infinitely
                // zero-padded empty string, constrained by max_len.
                //
                sources.push(Bytes::new());
                continue;
            };

            let value = match Self::value_to_bytes(&snapshot.value.data) {
                Ok(value) => value,

                Err(error) => {
                    return (
                        MochaOperation::Abort,
                        CacheCatError::from(error).into(),
                    );
                }
            };

            sources.push(value);
        }

        //
        // Redis BITOP result size is the size of the longest input string.
        //
        let max_len = sources
            .iter()
            .map(Bytes::len)
            .max()
            .unwrap_or(0);

        //
        // Redis doesn't retain an empty destination value for BITOP when all
        // source strings are empty/missing.
        //
        // It deletes the destination and returns 0.
        //
        if max_len == 0 {
            return (
                MochaOperation::Remove,
                Value::Integer(0),
            );
        }

        let result = self.compute(&sources, max_len);

        debug_assert_eq!(result.len(), max_len);

        //
        // Important:
        //
        // Do NOT remove destination merely because all result bytes are zero.
        //
        // For example:
        //
        //   SET a "\xff"
        //   SET b "\x00"
        //   BITOP AND dst a b
        //
        // dst must exist and contain one byte 0x00.
        //
        // Only max_len == 0 results in deletion.
        //
        let value = MyValue::new(ValueObject::String(Bytes::from(result)));

        (
            MochaOperation::Insert {
                value,

                //
                // BITOP overwrites destination, so previous TTL is discarded.
                //
                expire: ExpirePolicy::Persistent,
            },

            //
            // Redis returns the destination string length in BYTES.
            //
            Value::Integer(max_len as i64),
        )
    }
}