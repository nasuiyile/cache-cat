//! Redis hash floating-point syntax and `LD_STR_HUMAN` formatting.
//!
//! Software x87 arithmetic keeps parsing, addition, and snapshot replay
//! identical across hosts, including hosts whose native long double is f64.

use bytes::Bytes;
use rustc_apfloat::ieee::X87DoubleExtended;
use rustc_apfloat::{Float, Round, Status};

const MAX_LONG_DOUBLE_CHARS: usize = 5 * 1024;
const FRACTION_DIGITS: u32 = 17;
const DECIMAL_SCALE: u128 = 10_u128.pow(FRACTION_DIGITS);
const FIVE_POW_17: u128 = 5_u128.pow(FRACTION_DIGITS);

/// Parse Redis's C-locale long-double syntax without accepting partial input.
/// Explicit infinities are valid numbers; the command rejects non-finite sums.
pub(super) fn parse(bytes: &[u8]) -> Option<X87DoubleExtended> {
    if bytes.is_empty() || bytes.len() >= MAX_LONG_DOUBLE_CHARS {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    let unsigned = bytes
        .strip_prefix(b"-")
        .or_else(|| bytes.strip_prefix(b"+"))
        .unwrap_or(bytes);
    if unsigned.eq_ignore_ascii_case(b"inf") || unsigned.eq_ignore_ascii_case(b"infinity") {
        return Some(if bytes[0] == b'-' {
            -X87DoubleExtended::INFINITY
        } else {
            X87DoubleExtended::INFINITY
        });
    }

    let hexadecimal = unsigned.starts_with(b"0x") || unsigned.starts_with(b"0X");
    let mantissa = if hexadecimal {
        &unsigned[2..]
    } else {
        unsigned
    };
    let is_digit = |byte: u8| {
        if hexadecimal {
            byte.is_ascii_hexdigit()
        } else {
            byte.is_ascii_digit()
        }
    };
    let mut position = 0;
    let mut digits = 0;
    while position < mantissa.len() && is_digit(mantissa[position]) {
        position += 1;
        digits += 1;
    }
    if mantissa.get(position) == Some(&b'.') {
        position += 1;
        while position < mantissa.len() && is_digit(mantissa[position]) {
            position += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    let has_exponent = position < mantissa.len();
    if has_exponent {
        let exponent_marker = if hexadecimal { b'p' } else { b'e' };
        if mantissa[position].to_ascii_lowercase() != exponent_marker {
            return None;
        }
        position += 1;
        if matches!(mantissa.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let exponent = &mantissa[position..];
        if exponent.is_empty() || !exponent.iter().all(u8::is_ascii_digit) {
            return None;
        }
    }

    // C's strtold accepts hexadecimal values without a binary exponent;
    // apfloat requires one. Decimal exponent syntax is checked above because
    // apfloat also accepts an incomplete exponent such as "1e+".
    let normalized;
    let text = if hexadecimal && !has_exponent {
        normalized = format!("{text}p0");
        normalized.as_str()
    } else {
        text
    };
    let parsed = X87DoubleExtended::from_str_r(text, Round::NearestTiesToEven).ok()?;
    if parsed
        .status
        .intersects(Status::OVERFLOW | Status::INVALID_OP)
        || (parsed.status.contains(Status::UNDERFLOW) && parsed.value.is_zero())
        || parsed.value.is_nan()
    {
        None
    } else {
        Some(parsed.value)
    }
}

/// Format a finite value like Redis's `%.17Lf`, then trim fractional zeroes.
pub(super) fn format(value: X87DoubleExtended) -> Bytes {
    debug_assert!(value.is_finite());
    let bits = value.to_bits();
    let significand = bits as u64 as u128;
    let biased_exponent = ((bits >> 64) & 0x7fff) as i32;
    let binary_exponent = biased_exponent.max(1) - 16383 - 63;

    if binary_exponent >= 0 {
        // These values are exact integers. apfloat's display implementation
        // can print their full decimal expansion without rounding. Its width
        // controls the exponent threshold, not padding. An x87 finite value
        // has at most 4933 integer digits, below this shared Redis limit.
        return format!(
            "{value:width$.precision$}",
            width = MAX_LONG_DOUBLE_CHARS,
            precision = MAX_LONG_DOUBLE_CHARS,
        )
        .into();
    }

    // value * 10^17 = significand * 5^17 * 2^(binary_exponent + 17).
    // The product needs at most 104 bits. A left shift here is at most 16
    // bits, so all fractional-value formatting fits in u128 exactly.
    let scaled = significand * FIVE_POW_17;
    let shift = binary_exponent + FRACTION_DIGITS as i32;
    let rounded = if shift >= 0 {
        scaled << shift
    } else if -shift >= 128 {
        0
    } else {
        let shift = (-shift) as u32;
        let integer = scaled >> shift;
        let remainder = scaled & ((1_u128 << shift) - 1);
        let halfway = 1_u128 << (shift - 1);
        integer + u128::from(remainder > halfway || (remainder == halfway && integer & 1 == 1))
    };
    if rounded == 0 {
        return Bytes::from_static(b"0");
    }

    let sign = if value.is_negative() { "-" } else { "" };
    let integer = rounded / DECIMAL_SCALE;
    let fraction = rounded % DECIMAL_SCALE;
    if fraction == 0 {
        return format!("{sign}{integer}").into();
    }
    let mut result = format!("{sign}{integer}.{fraction:017}");
    result.truncate(result.trim_end_matches('0').len());
    result.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number(text: &str) -> X87DoubleExtended {
        parse(text.as_bytes()).unwrap()
    }

    #[test]
    fn accepts_decimal_and_c_hexadecimal_syntax() {
        for (input, expected) in [
            ("+01.50", "1.5"),
            (".5", "0.5"),
            ("1.", "1"),
            ("-2.5e+2", "-250"),
            ("1E-2", "0.01"),
            ("0xA.8", "10.5"),
            ("-0X1.8p+1", "-3"),
            ("0x.8p0", "0.5"),
            ("0x1.", "1"),
            ("0e999999999999999999999", "0"),
        ] {
            assert_eq!(
                format(number(input)).as_ref(),
                expected.as_bytes(),
                "{input}"
            );
        }
        for input in ["inf", "+INF", "Infinity", "-iNfInItY"] {
            let parsed = number(input);
            assert!(parsed.is_infinite(), "{input}");
            assert_eq!(parsed.is_negative(), input.starts_with('-'));
        }
    }

    #[test]
    fn rejects_partial_numbers_non_ascii_and_nan() {
        for input in [
            "", "+", "-", ".", " 1", "1 ", "\t1", "1\n", "1\0", "1\0x", "++1", "+-1", "--1", "1_0",
            "１", "1e", "1e+", "1e-", "1e1.0", "0x", "0xp0", "0x.p0", "0x1p", "0x1p+", "0x1p1.0",
            "NaN", "-nan", "+nan", "nan(1)", "snan", "infinite", "1..0",
        ] {
            assert!(parse(input.as_bytes()).is_none(), "accepted {input:?}");
        }
        assert!(parse(&[0xff]).is_none());
        assert!(parse(&vec![b'0'; MAX_LONG_DOUBLE_CHARS]).is_none());
        assert!(parse(&vec![b'0'; MAX_LONG_DOUBLE_CHARS - 1]).is_some());
    }

    #[test]
    fn rejects_overflow_and_underflow_to_zero_but_accepts_subnormal_values() {
        for input in [
            "1e5000",
            "-1e5000",
            "1e-5000",
            "-1e-5000",
            "0x1p16384",
            "0x1p-16446",
            "1e99999999999999999999",
            "1e-99999999999999999999",
        ] {
            assert!(parse(input.as_bytes()).is_none(), "accepted {input}");
        }
        assert_eq!(number("0x1p-16445").to_bits(), 1);
        assert!(!number("1e-4950").is_zero());
        assert!(number("0e-99999999999999999999").is_zero());
    }

    #[test]
    fn preserves_extended_precision_when_adding() {
        for (left, right, expected) in [
            ("0.1", "0.2", "0.3"),
            ("9007199254740992", "1", "9007199254740993"),
            ("9223372036854775807", "1", "9223372036854775808"),
            ("-9223372036854775808", "-1", "-9223372036854775809"),
            ("10.50", "0.1", "10.6"),
            ("5.0e3", "2.0e2", "5200"),
        ] {
            let sum = (number(left) + number(right)).value;
            assert_eq!(
                format(sum).as_ref(),
                expected.as_bytes(),
                "{left} + {right}"
            );
        }
    }

    #[test]
    fn rounds_to_seventeen_fractional_digits_with_ties_to_even() {
        for (input, expected) in [
            ("0x1p-18", "0.00000381469726562"),
            ("0x3p-18", "0.00001144409179688"),
            ("-0x1p-18", "-0.00000381469726562"),
            ("-0x3p-18", "-0.00001144409179688"),
            ("0.999999999999999999", "1"),
            ("1e-17", "0.00000000000000001"),
            ("1e-18", "0"),
            ("-1e-18", "0"),
            ("-0", "0"),
            ("0x1p-16445", "0"),
        ] {
            assert_eq!(
                format(number(input)).as_ref(),
                expected.as_bytes(),
                "{input}"
            );
        }
    }

    #[test]
    fn formats_large_integers_without_scientific_notation_or_precision_loss() {
        assert_eq!(
            format(number("0x1p256")).as_ref(),
            b"115792089237316195423570985008687907853269984665640564039457584007913129639936"
        );
        for input in ["0xf.fffffffffffffffp16380", "-0xf.fffffffffffffffp16380"] {
            let value = number(input);
            let formatted = format(value);
            assert_eq!(formatted.len(), 4933 + usize::from(value.is_negative()));
            assert!(!formatted.contains(&b'e'));
            assert!(!formatted.contains(&b'.'));
            assert_eq!(parse(&formatted).unwrap().to_bits(), value.to_bits());
        }
    }
}
