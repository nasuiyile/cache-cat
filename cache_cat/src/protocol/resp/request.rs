//! Redis command requests are flat arrays of bulk strings, even in RESP3.
//! Keep their parser non-recursive; the general decoder is for replies.

use super::{AggregateKind, Parser, RespParseError};
use crate::raft::types::core::response_value::Value;
use crate::utils::parse_canonical_i64;
use bytes::{Buf, BytesMut};

// Redis's default protocol limits, checked before allocating argument metadata.
const MAX_ARGUMENTS: i64 = i32::MAX as i64;
const MAX_BULK_LENGTH: i64 = 512 * 1024 * 1024;

/// Decode one command without consuming an incomplete or invalid request.
/// Empty multibulk requests are ignored, as in Redis's processMultibulkBuffer.
pub fn take_request_from_bytes_stream(
    buffer: &mut BytesMut,
) -> Result<Option<Value>, RespParseError> {
    loop {
        let Some(meta) = parse_request(buffer)? else {
            return Ok(None);
        };
        let len = meta.len();
        if matches!(&meta, Parser::Aggregate { value: Some((_, elements)), .. } if elements.is_empty())
        {
            buffer.advance(len);
            continue;
        }
        return Ok(Some(meta.take(buffer.split_to(len).freeze())));
    }
}

fn parse_request(buffer: &[u8]) -> Result<Option<Parser>, RespParseError> {
    let Some(&marker) = buffer.first() else {
        return Ok(None);
    };
    if marker != b'*' {
        return Err(RespParseError::ExpectedArray(marker));
    }
    let Some((line, pos)) = Parser::read_line(buffer) else {
        return Ok(None);
    };
    let count = parse_canonical_i64(line).ok_or(RespParseError::InvalidArrayLength)?;
    if count > MAX_ARGUMENTS {
        return Err(RespParseError::InvalidArrayLength);
    }
    let count = count.max(0) as usize;
    let mut full = pos;
    let mut elements = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        let Some(&marker) = buffer.get(full) else {
            return Ok(None);
        };
        // Reject nested aggregates as soon as their marker arrives, before
        // parsing an inner header, allocating children, or waiting for EOF.
        if marker != b'$' {
            return Err(RespParseError::ExpectedBulk(marker));
        }
        let Some((line, header_len)) = Parser::read_line(&buffer[full..]) else {
            return Ok(None);
        };
        let length = parse_canonical_i64(line).ok_or(RespParseError::InvalidBulkLength)?;
        if !(0..=MAX_BULK_LENGTH).contains(&length) {
            return Err(RespParseError::InvalidBulkLength);
        }
        let length = length as usize;
        let end = full
            .checked_add(header_len)
            .and_then(|start| start.checked_add(length))
            .and_then(|end| end.checked_add(2))
            .ok_or(RespParseError::InvalidBulkLength)?;
        if end > buffer.len() {
            return Ok(None);
        }
        if &buffer[end - 2..end] != b"\r\n" {
            return Err(RespParseError::InvalidBulkTerminator);
        }
        elements.push(Parser::Bytes {
            len: end - full,
            bytes: Some((header_len, length)),
        });
        full = end;
    }
    Ok(Some(Parser::Aggregate {
        kind: AggregateKind::Array,
        len: full,
        value: Some((pos, elements)),
    }))
}

#[cfg(test)]
#[path = "request_tests.rs"]
mod tests;
