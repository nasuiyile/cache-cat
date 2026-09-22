/// Parse bytes as i64, returns None if invalid
#[inline]
pub fn parse_i64(data: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(data).ok()?;
    s.trim().parse::<i64>().ok()
}

/// Parse bytes as i64 only when they are the canonical decimal form of that
/// number, mirroring Redis `string2ll`: no surrounding whitespace, no `+` sign,
/// no leading zeros and no `-0`.
///
/// This decides whether a string may be stored with the integer encoding: the
/// encoding must round-trip byte-for-byte, otherwise `SET k "01"` would read
/// back as `"1"`.
#[inline]
pub fn parse_canonical_i64(data: &[u8]) -> Option<i64> {
    // `i64::MIN` is 20 bytes long; anything longer cannot be canonical.
    if data.len() > 20 {
        return None;
    }
    let digits = data.strip_prefix(b"-").unwrap_or(data);
    match digits {
        // "0" is canonical, "-0" is not.
        [b'0'] if digits.len() == data.len() => return Some(0),
        [b'1'..=b'9', ..] => {}
        _ => return None,
    }
    // The remaining bytes must all be digits; `parse` checks that and overflow.
    std::str::from_utf8(data).ok()?.parse::<i64>().ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_i64_accepts_only_round_trippable_forms() {
        let cases: [(&[u8], i64); 6] = [
            (b"0", 0),
            (b"1", 1),
            (b"-1", -1),
            (b"10", 10),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];
        for (input, expected) in cases {
            assert_eq!(parse_canonical_i64(input), Some(expected));
            assert_eq!(expected.to_string().as_bytes(), input);
        }
    }

    #[test]
    fn canonical_i64_rejects_forms_that_would_be_rewritten() {
        let cases: [&[u8]; 18] = [
            b"",
            b"01",
            b"00",
            b"-0",
            b"-01",
            b"+1",
            b" 1",
            b"1 ",
            b"1\n",
            b"-",
            b"--1",
            b"1.0",
            b"1e3",
            b"0x10",
            b"abc",
            b"9223372036854775808",
            b"-9223372036854775809",
            b"123456789012345678901",
        ];
        for input in cases {
            assert_eq!(parse_canonical_i64(input), None, "{:?}", input);
        }
    }
}
