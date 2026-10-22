//! Argument parsing and reply shape shared by SCAN, HSCAN, SSCAN and ZSCAN.

use crate::error::ProtocolError;
use crate::protocol::option_eq;
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

/// Page size Redis uses when COUNT is absent.
const DEFAULT_COUNT: usize = 10;

/// The `cursor [MATCH pattern] [COUNT count]` options every SCAN command takes.
pub struct ScanOptions {
    pub cursor: u64,
    pub count: usize,
    /// None means every element matches, so no glob matcher is built.
    pub pattern: Option<Bytes>,
}

impl ScanOptions {
    /// Parse the `[MATCH pattern] [COUNT count]` options after the cursor,
    /// letting `extra` consume command specific ones and report how many
    /// arguments it used.
    pub fn parse(
        cursor: u64,
        options: &[Value],
        mut extra: impl FnMut(&[u8], &[Value]) -> Result<Option<usize>, ProtocolError>,
    ) -> Result<Self, ProtocolError> {
        let mut count = DEFAULT_COUNT;
        let mut pattern = None;
        let mut index = 0;
        while index < options.len() {
            let name = options[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;

            if option_eq(&name, b"match") || option_eq(&name, b"count") {
                let Some(value) = options.get(index + 1).and_then(Value::string_bytes_clone) else {
                    return Err(ProtocolError::SyntaxError);
                };
                if option_eq(&name, b"match") {
                    // A bare "*" is Redis' way of disabling the pattern.
                    pattern = (value.as_ref() != b"*".as_slice()).then_some(value);
                } else {
                    count = parse_count(&value)?;
                }
                index += 2;
            } else {
                let consumed =
                    extra(&name, &options[index + 1..])?.ok_or(ProtocolError::SyntaxError)?;
                index += 1 + consumed;
            }
        }

        Ok(Self {
            cursor,
            count,
            pattern,
        })
    }
}

/// The `[cursor, elements]` reply, identical in RESP2 and RESP3.
pub fn reply(cursor: u64, elements: Vec<Value>) -> Value {
    Value::Array(Some(vec![
        Value::BulkString(Some(cursor.to_string().into())),
        Value::Array(Some(elements)),
    ]))
}

/// Redis reads the cursor as an unsigned decimal and rejects anything else.
pub fn parse_cursor(value: &Value) -> Result<u64, ProtocolError> {
    value
        .string_bytes_clone()
        .and_then(|text| std::str::from_utf8(&text).ok()?.parse().ok())
        .ok_or_else(|| ProtocolError::response("ERR invalid cursor"))
}

/// COUNT is a signed value like Redis' long, so only 1 and above are valid.
fn parse_count(value: &Bytes) -> Result<usize, ProtocolError> {
    let count: i64 = std::str::from_utf8(value)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(ProtocolError::NotAnInteger)?;
    if count < 1 {
        return Err(ProtocolError::SyntaxError);
    }
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}
