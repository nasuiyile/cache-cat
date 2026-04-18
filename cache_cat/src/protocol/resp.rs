use crate::raft::types::core::response_value::Value;

pub struct Parser;

impl Parser {
    /// Parse RESP data from buffer, return (Value, consumed_bytes) if successful
    pub fn parse(buffer: &[u8]) -> Option<(Value, usize)> {
        if buffer.is_empty() {
            return None;
        }

        let mut pos = 0;
        let result = Self::parse_value(buffer, &mut pos)?;
        Some((result, pos))
    }
    fn parse_value(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        if *pos >= buffer.len() {
            return None;
        }

        let type_byte = buffer[*pos];
        *pos += 1;

        match type_byte {
            b'+' => Self::parse_simple_string(buffer, pos),
            b'-' => Self::parse_error(buffer, pos),
            b':' => Self::parse_integer(buffer, pos),
            b'$' => Self::parse_bulk_string(buffer, pos),
            b'*' => Self::parse_array(buffer, pos),
            _ => None,
        }
    }
    fn parse_simple_string(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        let line = Self::read_line(buffer, pos)?;
        Some(Value::SimpleString(
            String::from_utf8_lossy(line).to_string(),
        ))
    }

    fn parse_error(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        let line = Self::read_line(buffer, pos)?;
        Some(Value::Error(String::from_utf8_lossy(line).to_string()))
    }

    fn parse_integer(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        let line = Self::read_line(buffer, pos)?;
        let num = String::from_utf8_lossy(line).parse::<i64>().ok()?;
        Some(Value::Integer(num))
    }

    fn parse_bulk_string(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        let line = Self::read_line(buffer, pos)?;
        let len = String::from_utf8_lossy(line).parse::<i64>().ok()?;

        if len == -1 {
            return Some(Value::BulkString(None));
        }

        if len < 0 {
            return None;
        }

        let len = len as usize;

        // Check if we have enough data (len + \r\n)
        if *pos + len + 2 > buffer.len() {
            return None;
        }

        let data = buffer[*pos..*pos + len].to_vec();
        *pos += len + 2; // +2 for \r\n

        Some(Value::BulkString(Some(data)))
    }

    fn parse_array(buffer: &[u8], pos: &mut usize) -> Option<Value> {
        let line = Self::read_line(buffer, pos)?;
        let count = String::from_utf8_lossy(line).parse::<i64>().ok()?;

        if count == -1 {
            return Some(Value::Array(None));
        }

        if count < 0 {
            return None;
        }

        let count = count as usize;
        let mut items = Vec::with_capacity(count);

        for _ in 0..count {
            items.push(Self::parse_value(buffer, pos)?);
        }

        Some(Value::Array(Some(items)))
    }

    fn read_line<'a>(buffer: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
        let start = *pos;

        // Find \r\n
        for i in start..buffer.len().saturating_sub(1) {
            if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
                *pos = i + 2;
                return Some(&buffer[start..i]);
            }
        }

        None
    }
}
