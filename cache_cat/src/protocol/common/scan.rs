//! Parsing shared by Redis collection scan commands.

use crate::utils::parse_canonical_i64;

/// Redis's string2ull first tries a canonical signed decimal, then falls
/// back to strtoull. Unlike COUNT, cursors accept a leading '+', leading
/// zeros/space and C-string termination through that fallback.
pub(crate) fn parse_cursor(value: &[u8]) -> Option<u64> {
    let value = &value[..value
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(value.len())];
    if let Some(integer) = parse_canonical_i64(value) {
        return u64::try_from(integer).ok();
    }
    let start = value
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))?;
    let mut digits = &value[start..];
    let negative = digits[0] == b'-';
    if matches!(digits[0], b'+' | b'-') {
        digits = &digits[1..];
    }
    if digits.is_empty() {
        return None;
    }
    let mut result = 0u64;
    for &digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        result = result.checked_mul(10)?.checked_add((digit - b'0') as u64)?;
    }
    Some(if negative {
        result.wrapping_neg()
    } else {
        result
    })
}
