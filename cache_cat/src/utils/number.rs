/// Parse bytes as i64, returns None if invalid
pub fn parse_i64(data: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(data).ok()?;
    s.trim().parse::<i64>().ok()
}