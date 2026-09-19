/// Parse bytes as i64, returns None if invalid
#[inline]
pub fn parse_i64(data: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(data).ok()?;
    s.trim().parse::<i64>().ok()
}

/// Parse bytes as an IEEE-754 double, returning `None` for invalid input.
#[inline]
pub fn parse_f64(data: &[u8]) -> Option<f64> {
    std::str::from_utf8(data).ok()?.parse::<f64>().ok()
}

#[inline(always)]
pub fn merge_u64(high_48: u64, low_16: u16) -> u64 {
    ((high_48 & 0xFFFF_FFFF_FFFF) << 16) | (low_16 as u64)
}
