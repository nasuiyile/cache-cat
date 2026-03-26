use crate::network::model::Response;

pub struct Parser;

impl Parser {
    /// Parse RESP data from buffer, return (Value, consumed_bytes) if successful
    pub fn parse(buffer: &[u8]) -> Option<(Response, usize)> {
        if buffer.is_empty() {
            return None;
        }

        let mut pos = 0;
        let result = Self::parse_value(buffer, &mut pos)?;
        Some((result, pos))
    }
    fn parse_value(buffer: &[u8], pos: &mut usize) -> Option<Response> {
        if *pos >= buffer.len() {
            return None;
        }

        let type_byte = buffer[*pos];
        *pos += 1;

        match type_byte {

            _ => None,
        }
    }
}
