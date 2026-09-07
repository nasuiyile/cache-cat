use bytes::{Buf, Bytes, BytesMut};

use crate::raft::types::core::response_value::Value;

/// Which aggregate frame a parsed header belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    /// `*` array (RESP2 / RESP3)
    Array,
    /// `%` map (RESP3), children are stored flattened: 2N elements
    Map,
    /// `~` set (RESP3)
    Set,
    /// `>` push (RESP3)
    Push,
    /// `|` attribute (RESP3), children are stored flattened: 2N elements.
    /// An attribute is metadata attached to the *next* element; the decoder
    /// parses and discards it, returning the element that follows.
    Attribute,
}

pub enum Parser {
    String {
        /// full size with `mode`, `line`, `eof`.
        len: usize,
    },
    Error {
        /// full size with `mode`, `line`, `eof`.
        len: usize,
    },
    Integer {
        /// full size with `mode`, `line`, `eof`.
        len: usize,
        // the value of integer
        value: i64,
    },
    Bytes {
        /// full size with `length-line`, `bytes`, `eof`.
        len: usize,
        /// `Some((pos, length))`
        bytes: Option<(usize, usize)>,
    },
    /// `!` bulk error (RESP3). Same layout as `Bytes`.
    BulkError {
        len: usize,
        /// `(pos, length)`
        bytes: (usize, usize),
    },
    /// `=` verbatim string (RESP3). Same layout as `Bytes`; the payload
    /// starts with a 3-character format and a `:` separator.
    Verbatim {
        len: usize,
        /// `(pos, length)` of the *full* payload (format prefix included)
        bytes: (usize, usize),
    },
    /// `_` null (RESP3)
    Null { len: usize },
    /// `#` boolean (RESP3)
    Boolean { len: usize, value: bool },
    /// `,` double (RESP3)
    Double { len: usize, value: f64 },
    /// `(` big number (RESP3); the digits are re-read from the buffer.
    BigNumber { len: usize },
    /// `*` / `%` / `~` / `>` / `|` aggregates.
    Aggregate {
        kind: AggregateKind,
        /// full size with `mode`, `data`, `eof`.
        len: usize,
        /// `Some((pos, elements))`; `None` is the RESP2 null array `*-1`.
        value: Option<(usize, Vec<Parser>)>,
    },
}

impl Parser {
    /// get the length of full parsed element,
    /// with `mode`, `data`, `eof` and so on.
    #[inline]
    pub const fn len(&self) -> usize {
        match self {
            Parser::String { len }
            | Parser::Error { len }
            | Parser::Integer { len, .. }
            | Parser::Bytes { len, .. }
            | Parser::BulkError { len, .. }
            | Parser::Verbatim { len, .. }
            | Parser::Null { len }
            | Parser::Boolean { len, .. }
            | Parser::Double { len, .. }
            | Parser::BigNumber { len }
            | Parser::Aggregate { len, .. } => *len,
        }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn take_from_bytes_stream(buffer: &mut BytesMut) -> Option<Value> {
        // check has `mode` (1 byte) and `eof` (2 bytes)
        if buffer.len() < 3 {
            return None;
        }

        // parse the metadata of element
        let meta = Self::parse_meta(buffer)?;
        let len = meta.len();
        // split the buffer data
        let buffer = buffer.split_to(len).freeze();
        // take the value
        Some(meta.take(buffer))
    }

    fn take(self, mut buffer: Bytes) -> Value {
        match self {
            Parser::String { len } => {
                Value::SimpleString(String::from_utf8_lossy(&buffer[1..len - 2]).into_owned())
            }
            Parser::Error { len } => {
                Value::Error(String::from_utf8_lossy(&buffer[1..len - 2]).into_owned())
            }
            Parser::Integer { value, .. } => Value::Integer(value),
            Parser::Bytes { bytes: None, .. } => Value::BulkString(None),
            Parser::Bytes {
                bytes: Some((pos, len)),
                ..
            } => {
                buffer.advance(pos);
                let data = buffer.split_to(len);
                buffer.advance(2);
                Value::BulkString(Some(data))
            }
            Parser::BulkError {
                bytes: (pos, len), ..
            } => {
                buffer.advance(pos);
                let data = buffer.split_to(len);
                Value::BulkError(String::from_utf8_lossy(&data).into_owned())
            }
            Parser::Verbatim {
                bytes: (pos, len), ..
            } => {
                buffer.advance(pos);
                let payload = buffer.split_to(len);
                // The payload is `xxx:<data>` where `xxx` is the format.
                if len >= 4 && payload[3] == b':' {
                    let format = String::from_utf8_lossy(&payload[..3]).into_owned();
                    let data = payload.slice(4..);
                    Value::VerbatimString { format, data }
                } else {
                    // Malformed prefix: expose the raw payload.
                    Value::VerbatimString {
                        format: String::new(),
                        data: payload,
                    }
                }
            }
            Parser::Null { .. } => Value::Null,
            Parser::Boolean { value, .. } => Value::Boolean(value),
            Parser::Double { value, .. } => Value::Double(value),
            Parser::BigNumber { len } => {
                Value::BigNumber(String::from_utf8_lossy(&buffer[1..len - 2]).into_owned())
            }
            Parser::Aggregate { value: None, .. } => Value::Array(None),
            Parser::Aggregate {
                kind,
                value: Some((pos, elements)),
                ..
            } => {
                // split `count` line
                buffer.advance(pos);
                // take values
                let mut values = elements
                    .into_iter()
                    .map(|element| {
                        let len = element.len();
                        let chunk = buffer.split_to(len);
                        element.take(chunk)
                    })
                    .collect::<Vec<_>>();
                match kind {
                    AggregateKind::Array => Value::Array(Some(values)),
                    AggregateKind::Set => Value::Set(values),
                    AggregateKind::Push => Value::Push(values),
                    AggregateKind::Map => {
                        let mut pairs = Vec::with_capacity(values.len() / 2);
                        let mut iter = values.drain(..);
                        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                            pairs.push((k, v));
                        }
                        Value::Map(pairs)
                    }
                    AggregateKind::Attribute => {
                        // The attribute map (2N leading elements) is metadata
                        // for the trailing element: discard it and return the
                        // annotated element, like most Redis clients do.
                        values.pop().unwrap_or(Value::Null)
                    }
                }
            }
        }
    }

    fn parse_meta(buffer: &[u8]) -> Option<Parser> {
        // check has `mode` (1 byte) and `eof` (2 bytes)
        if buffer.len() < 3 {
            return None;
        }
        let mode = buffer[0];
        match mode {
            b'+' => Self::parse_simple_string(buffer),
            b'-' => Self::parse_error(buffer),
            b':' => Self::parse_integer(buffer),
            b'$' => Self::parse_bulk_string(buffer),
            b'*' => Self::parse_aggregate(buffer, AggregateKind::Array),
            // ---- RESP3 ----
            b'_' => Self::parse_null(buffer),
            b'#' => Self::parse_boolean(buffer),
            b',' => Self::parse_double(buffer),
            b'(' => Self::parse_big_number(buffer),
            b'!' => Self::parse_bulk_error(buffer),
            b'=' => Self::parse_verbatim(buffer),
            b'%' => Self::parse_aggregate(buffer, AggregateKind::Map),
            b'~' => Self::parse_aggregate(buffer, AggregateKind::Set),
            b'>' => Self::parse_aggregate(buffer, AggregateKind::Push),
            b'|' => Self::parse_attribute(buffer),
            _ => None,
        }
    }

    /// Read `line` from buffer, start with `mode`,
    /// end with `eof` ( `\r\n` ), return `Some((line, len))`.
    /// Returned `len` includes full size of `mode`, `line`, `eof`.
    ///
    /// ```text
    ///  1 byte       n bytes       2 bytes
    /// ┌──────┬────────────────────┬─────┐
    /// │ mode │       line         │ eof │
    /// └──────┴────────────────────┴─────┘
    /// ```
    #[inline]
    fn read_line(buffer: &[u8]) -> Option<(&[u8], usize)> {
        let index = Self::find_line(&buffer[1..])?;
        Some((&buffer[1..index + 1], index + 3))
    }

    /// find the index of `eof` ( `\r\n` ),
    /// also the length of `line`
    #[inline]
    fn find_line(buffer: &[u8]) -> Option<usize> {
        for (index, window) in buffer.windows(2).enumerate() {
            if window == b"\r\n" {
                return Some(index);
            }
        }
        None
    }

    /// Parse simple string from buffer using `line`.
    fn parse_simple_string(buffer: &[u8]) -> Option<Parser> {
        let (_, len) = Self::read_line(buffer)?;
        Some(Parser::String { len })
    }

    /// Parse error string from buffer using `line`.
    fn parse_error(buffer: &[u8]) -> Option<Parser> {
        let (_, len) = Self::read_line(buffer)?;
        Some(Parser::Error { len })
    }

    /// Return `Some((value, len))`.
    /// Returned `len` includes full size of `mode`, `line`, `eof`.
    #[inline]
    fn read_i64(buffer: &[u8]) -> Option<(i64, usize)> {
        let (line, len) = Self::read_line(buffer)?;
        let value = str::from_utf8(line).ok()?.parse::<i64>().ok()?;
        Some((value, len))
    }

    fn parse_integer(buffer: &[u8]) -> Option<Parser> {
        let (value, len) = Self::read_i64(buffer)?;
        Some(Parser::Integer { len, value })
    }

    /// Parse `_\r\n` (RESP3 null). The line must be empty.
    fn parse_null(buffer: &[u8]) -> Option<Parser> {
        let (line, len) = Self::read_line(buffer)?;
        if !line.is_empty() {
            return None;
        }
        Some(Parser::Null { len })
    }

    /// Parse `#t\r\n` / `#f\r\n` (RESP3 boolean).
    fn parse_boolean(buffer: &[u8]) -> Option<Parser> {
        let (line, len) = Self::read_line(buffer)?;
        let value = match line {
            b"t" => true,
            b"f" => false,
            _ => return None,
        };
        Some(Parser::Boolean { len, value })
    }

    /// Parse `,3.14\r\n` (RESP3 double), including `inf`, `-inf` and `nan`.
    fn parse_double(buffer: &[u8]) -> Option<Parser> {
        let (line, len) = Self::read_line(buffer)?;
        let text = str::from_utf8(line).ok()?;
        let value = match text {
            "inf" | "+inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" | "-nan" => f64::NAN,
            other => other.parse::<f64>().ok()?,
        };
        Some(Parser::Double { len, value })
    }

    /// Parse `(3492890328409238509324850943850943825024385\r\n` (big number).
    fn parse_big_number(buffer: &[u8]) -> Option<Parser> {
        let (line, len) = Self::read_line(buffer)?;
        // Validate: optional sign followed by at least one digit.
        let digits = match line.first() {
            Some(b'+') | Some(b'-') => &line[1..],
            _ => line,
        };
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        Some(Parser::BigNumber { len })
    }

    /// Read a length-prefixed blob (`$` / `!` / `=` share this layout).
    ///
    ///  ```text
    ///  1 byte   n bytes   2 bytes   `len` bytes   2 bytes
    /// ┌──────┬──────────┬────────┬──────────────┬────────┐
    /// │ mode │   len    │  eof   │    bytes     │  eof   │
    /// └──────┴──────────┴────────┴──────────────┴────────┘
    /// ```
    ///
    /// Returns `Some((full_len, Some((pos, len))))`, or
    /// `Some((full_len, None))` for the RESP2 null bulk string `$-1`.
    fn read_blob(buffer: &[u8], allow_null: bool) -> Option<(usize, Option<(usize, usize)>)> {
        let (len, pos) = Self::read_i64(buffer)?;
        let len = match len {
            -1 if allow_null => return Some((pos, None)),
            // TODO: Handle the Error
            ..0 => return None,
            len => len as usize,
        };
        let full = pos + len + 2;
        if full > buffer.len() {
            // the data is not completed
            return None;
        }
        Some((full, Some((pos, len))))
    }

    /// Parse `bytes` from buffer.
    fn parse_bulk_string(buffer: &[u8]) -> Option<Parser> {
        let (len, bytes) = Self::read_blob(buffer, true)?;
        Some(Parser::Bytes { len, bytes })
    }

    /// Parse `!<len>\r\n<error>\r\n` (RESP3 bulk error).
    fn parse_bulk_error(buffer: &[u8]) -> Option<Parser> {
        let (len, bytes) = Self::read_blob(buffer, false)?;
        Some(Parser::BulkError { len, bytes: bytes? })
    }

    /// Parse `=<len>\r\ntxt:...\r\n` (RESP3 verbatim string).
    fn parse_verbatim(buffer: &[u8]) -> Option<Parser> {
        let (len, bytes) = Self::read_blob(buffer, false)?;
        Some(Parser::Verbatim { len, bytes: bytes? })
    }

    /// Parse an aggregate frame (`*` array, `%` map, `~` set, `>` push).
    ///
    /// ```text
    ///  1 byte   n bytes   2 bytes   1 byte  ...  2 bytes
    /// ┌──────┬──────────┬────────┬────────┬─────┬─────┐
    /// │ mode │  count   │  eof   │  mode  │ ... │ eof │
    /// └──────┴──────────┴────────┴────────┴─────┴─────┘
    /// ```
    ///
    /// For maps (and attributes) `count` is the number of key-value pairs,
    /// so 2 * count child elements follow.
    fn parse_aggregate(buffer: &[u8], kind: AggregateKind) -> Option<Parser> {
        let (count, pos) = Self::read_i64(buffer)?;
        let count = match count {
            // Only the RESP2 array supports the null form `*-1`.
            -1 if kind == AggregateKind::Array => {
                return Some(Parser::Aggregate {
                    kind,
                    len: pos,
                    value: None,
                });
            }
            // TODO: Handle the Error
            ..0 => return None,
            count => count as usize,
        };
        let element_count = match kind {
            AggregateKind::Map | AggregateKind::Attribute => count.checked_mul(2)?,
            _ => count,
        };
        let mut elements = Vec::with_capacity(element_count.min(4096));
        let mut full = pos;
        for _ in 0..element_count {
            let meta = Self::parse_meta(&buffer[full..])?;
            let len = meta.len();
            full += len;
            elements.push(meta);
        }
        Some(Parser::Aggregate {
            kind,
            len: full,
            value: Some((pos, elements)),
        })
    }

    /// Parse `|<count>\r\n` (RESP3 attribute): a map of metadata followed by
    /// the actual element the attribute annotates. The annotated element is
    /// stored as the last child so `take()` can return it directly.
    fn parse_attribute(buffer: &[u8]) -> Option<Parser> {
        let attrs = Self::parse_aggregate(buffer, AggregateKind::Attribute)?;
        let (attr_len, pos, mut elements) = match attrs {
            Parser::Aggregate {
                len,
                value: Some((pos, elements)),
                ..
            } => (len, pos, elements),
            _ => return None,
        };
        // Parse the element that follows the attribute map.
        let inner = Self::parse_meta(&buffer[attr_len..])?;
        let full = attr_len + inner.len();
        elements.push(inner);
        Some(Parser::Aggregate {
            kind: AggregateKind::Attribute,
            len: full,
            value: Some((pos, elements)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn decode(input: &[u8]) -> Option<Value> {
        let mut buf = BytesMut::from(input);
        Parser::take_from_bytes_stream(&mut buf)
    }

    #[test]
    fn test_decode_resp2_basics() {
        assert!(matches!(decode(b"+OK\r\n"), Some(Value::SimpleString(s)) if s == "OK"));
        assert!(matches!(decode(b"-ERR x\r\n"), Some(Value::Error(e)) if e == "ERR x"));
        assert!(matches!(decode(b":42\r\n"), Some(Value::Integer(42))));
        assert!(matches!(decode(b"$-1\r\n"), Some(Value::BulkString(None))));
        assert!(matches!(decode(b"*-1\r\n"), Some(Value::Array(None))));
    }

    #[test]
    fn test_decode_null() {
        assert!(matches!(decode(b"_\r\n"), Some(Value::Null)));
        // A null with garbage on the line is invalid.
        assert!(decode(b"_x\r\n").is_none());
    }

    #[test]
    fn test_decode_boolean() {
        assert!(matches!(decode(b"#t\r\n"), Some(Value::Boolean(true))));
        assert!(matches!(decode(b"#f\r\n"), Some(Value::Boolean(false))));
        assert!(decode(b"#x\r\n").is_none());
    }

    #[test]
    fn test_decode_double() {
        assert!(matches!(decode(b",1.23\r\n"), Some(Value::Double(d)) if d == 1.23));
        assert!(matches!(decode(b",10\r\n"), Some(Value::Double(d)) if d == 10.0));
        assert!(
            matches!(decode(b",inf\r\n"), Some(Value::Double(d)) if d.is_infinite() && d > 0.0)
        );
        assert!(
            matches!(decode(b",-inf\r\n"), Some(Value::Double(d)) if d.is_infinite() && d < 0.0)
        );
        assert!(matches!(decode(b",nan\r\n"), Some(Value::Double(d)) if d.is_nan()));
    }

    #[test]
    fn test_decode_big_number() {
        let raw = b"(3492890328409238509324850943850943825024385\r\n";
        assert!(matches!(
            decode(raw),
            Some(Value::BigNumber(n)) if n == "3492890328409238509324850943850943825024385"
        ));
        assert!(decode(b"(notanumber\r\n").is_none());
    }

    #[test]
    fn test_decode_bulk_error() {
        assert!(matches!(
            decode(b"!21\r\nSYNTAX invalid syntax\r\n"),
            Some(Value::BulkError(e)) if e == "SYNTAX invalid syntax"
        ));
    }

    #[test]
    fn test_decode_verbatim() {
        match decode(b"=15\r\ntxt:Some string\r\n") {
            Some(Value::VerbatimString { format, data }) => {
                assert_eq!(format, "txt");
                assert_eq!(&data[..], b"Some string");
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_decode_map() {
        match decode(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n") {
            Some(Value::Map(pairs)) => {
                assert_eq!(pairs.len(), 2);
                assert!(matches!(&pairs[0].0, Value::SimpleString(s) if s == "first"));
                assert!(matches!(pairs[1].1, Value::Integer(2)));
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_decode_set_and_push() {
        assert!(matches!(decode(b"~2\r\n:1\r\n:2\r\n"), Some(Value::Set(v)) if v.len() == 2));
        match decode(b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$5\r\nhello\r\n") {
            Some(Value::Push(v)) => assert_eq!(v.len(), 3),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_decode_attribute_is_discarded() {
        // |1<key-popularity -> map> followed by the actual reply :42
        let raw = b"|1\r\n+key-popularity\r\n:90\r\n:42\r\n";
        assert!(matches!(decode(raw), Some(Value::Integer(42))));
    }

    #[test]
    fn test_decode_incomplete_returns_none() {
        assert!(decode(b"%2\r\n+first\r\n:1\r\n").is_none());
        assert!(decode(b"$5\r\nab").is_none());
        assert!(decode(b",1.2").is_none());
    }

    #[test]
    fn test_roundtrip_resp3() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Double(2.5),
            Value::BigNumber("123456789012345678901234567890".into()),
            Value::Set(vec![Value::Integer(1)]),
            Value::Push(vec![Value::BulkString(Some(Bytes::from_static(b"x")))]),
            Value::Map(vec![(Value::Integer(1), Value::Boolean(false))]),
        ];
        for v in values {
            let mut buf = BytesMut::from(&v.encode_proto(3)[..]);
            let decoded = Parser::take_from_bytes_stream(&mut buf)
                .unwrap_or_else(|| panic!("failed to decode {:?}", v));
            assert!(buf.is_empty());
            // encode(decoded) == encode(v)
            assert_eq!(decoded.encode_proto(3), v.encode_proto(3), "{:?}", v);
        }
    }
}
