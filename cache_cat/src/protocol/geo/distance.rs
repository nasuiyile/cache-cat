//! Distances between positions and the units they are reported in, shared by
//! the GEO commands that measure or filter by distance.
//!
//! The trigonometry comes from the `libm` crate rather than from `f64`, whose
//! `sin`, `cos` and `asin` use the platform's C library and may round the last
//! bit differently from one platform or version to the next. A script can
//! write a distance it read, so every replica has to compute the same bits.

use crate::error::ProtocolError;
use crate::protocol::option_eq;
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Redis `EARTH_RADIUS_IN_METERS`: the Earth is taken as a perfect sphere.
pub(super) const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;
pub(super) const DEGREES_TO_RADIANS: f64 = std::f64::consts::PI / 180.0;
/// Fractional digits of a distance reply.
const DISTANCE_SCALE: f64 = 10000.0;

/// Unit of a distance argument or reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeoUnit {
    Meters,
    Kilometers,
    Feet,
    Miles,
}

impl GeoUnit {
    /// Redis `extractUnitOrReply`: the name is compared with `strcasecmp`.
    pub(super) fn parse(unit: &Value) -> Result<Self, ProtocolError> {
        let name = unit.string_bytes_clone().unwrap_or_default();
        if option_eq(&name, b"M") {
            Ok(GeoUnit::Meters)
        } else if option_eq(&name, b"KM") {
            Ok(GeoUnit::Kilometers)
        } else if option_eq(&name, b"FT") {
            Ok(GeoUnit::Feet)
        } else if option_eq(&name, b"MI") {
            Ok(GeoUnit::Miles)
        } else {
            Err(ProtocolError::response(
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ))
        }
    }

    /// Length of one unit in meters.
    pub(super) fn to_meters(self) -> f64 {
        match self {
            GeoUnit::Meters => 1.0,
            GeoUnit::Kilometers => 1000.0,
            GeoUnit::Feet => 0.3048,
            GeoUnit::Miles => 1609.34,
        }
    }
}

impl fmt::Display for GeoUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            GeoUnit::Meters => "m",
            GeoUnit::Kilometers => "km",
            GeoUnit::Feet => "ft",
            GeoUnit::Miles => "mi",
        })
    }
}

/// Redis `geohashGetDistance`: the great-circle distance in meters between two
/// positions given in degrees, by the haversine formula.
pub(super) fn distance(longitude1: f64, latitude1: f64, longitude2: f64, latitude2: f64) -> f64 {
    let longitude1 = longitude1 * DEGREES_TO_RADIANS;
    let longitude2 = longitude2 * DEGREES_TO_RADIANS;
    let v = libm::sin((longitude2 - longitude1) / 2.0);
    let latitude1 = latitude1 * DEGREES_TO_RADIANS;
    let latitude2 = latitude2 * DEGREES_TO_RADIANS;
    // Like Redis, measure along the meridian when the longitudes are equal.
    if v == 0.0 {
        return EARTH_RADIUS_IN_METERS * (latitude2 - latitude1).abs();
    }
    let u = libm::sin((latitude2 - latitude1) / 2.0);
    let a = u * u + libm::cos(latitude1) * libm::cos(latitude2) * v * v;
    2.0 * EARTH_RADIUS_IN_METERS * libm::asin(a.sqrt())
}

/// Redis `addReplyDoubleDistance`: a bulk string with four fractional digits
/// in both protocols.
///
/// As in `fixedpoint_d2string`, the scaled value is rounded half to even like
/// C's `llrint`, which differs from rounding the exact decimal expansion.
pub(super) fn distance_reply(distance: f64) -> Value {
    // No distance on Earth leaves the i64 range. C leaves the result of
    // `llrint` unspecified for a NaN; this conversion yields zero.
    let scaled = (distance * DISTANCE_SCALE).round_ties_even() as i64;
    let sign = if scaled < 0 { "-" } else { "" };
    let digits = scaled.unsigned_abs();
    let text = format!("{sign}{}.{:04}", digits / 10000, digits % 10000);
    Value::BulkString(Some(Bytes::from(text)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::geo::codec::decode_score;

    fn unit(name: &[u8]) -> Result<GeoUnit, ProtocolError> {
        GeoUnit::parse(&Value::BulkString(Some(Bytes::copy_from_slice(name))))
    }

    fn text(distance: f64) -> String {
        match distance_reply(distance) {
            Value::BulkString(Some(text)) => String::from_utf8(text.to_vec()).unwrap(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn units_are_matched_like_strcasecmp() {
        for (name, expected) in [
            (&b"m"[..], GeoUnit::Meters),
            (b"M", GeoUnit::Meters),
            (b"km", GeoUnit::Kilometers),
            (b"Km", GeoUnit::Kilometers),
            (b"ft", GeoUnit::Feet),
            (b"FT", GeoUnit::Feet),
            (b"mi", GeoUnit::Miles),
            (b"mI", GeoUnit::Miles),
            // C stops reading the name at a NUL.
            (b"km\0anything", GeoUnit::Kilometers),
        ] {
            assert_eq!(unit(name).unwrap(), expected, "{name:?}");
            // The name a unit prints is one it parses.
            assert_eq!(unit(expected.to_string().as_bytes()).unwrap(), expected);
        }
        for name in [&b""[..], b"k", b"mm", b"kms", b"meters", b" m", b"m ", b"1"] {
            assert_eq!(
                unit(name).unwrap_err().to_string(),
                "ERR unsupported unit provided. please use M, KM, FT, MI",
                "{name:?}"
            );
        }
    }

    #[test]
    fn redis_documentation_example_measures_the_same_distance() {
        // Scores GEOADD stores for Palermo and Catania.
        let (longitude1, latitude1) = decode_score(3479099956230698.0);
        let (longitude2, latitude2) = decode_score(3479447370796909.0);
        let meters = distance(longitude1, latitude1, longitude2, latitude2);
        for (unit, expected) in [
            (GeoUnit::Meters, "166274.1516"),
            (GeoUnit::Kilometers, "166.2742"),
            (GeoUnit::Miles, "103.3182"),
            (GeoUnit::Feet, "545518.8700"),
        ] {
            assert_eq!(text(meters / unit.to_meters()), expected, "{unit}");
        }
        // The distance does not depend on the direction.
        assert_eq!(
            distance(longitude2, latitude2, longitude1, latitude1),
            meters
        );
    }

    #[test]
    fn equal_longitudes_measure_along_the_meridian() {
        assert_eq!(distance(13.5, 38.0, 13.5, 38.0), 0.0);
        // One degree of latitude on a sphere of the Redis radius.
        let degree = EARTH_RADIUS_IN_METERS * DEGREES_TO_RADIANS;
        assert_eq!(text(degree), "111226.3000");
        assert_eq!(text(distance(-70.0, 10.0, -70.0, 11.0)), "111226.3000");
        assert_eq!(text(distance(-70.0, 11.0, -70.0, 10.0)), "111226.3000");
        // The general formula agrees to well below the printed precision.
        let nearby = distance(-70.0, 10.0, -70.0 + 1e-9, 11.0);
        assert!((nearby - degree).abs() < 1e-6, "{nearby}");
    }

    #[test]
    fn opposite_points_are_half_the_circumference_apart() {
        let half = std::f64::consts::PI * EARTH_RADIUS_IN_METERS;
        assert_eq!(text(half), "20020734.0000");
        // Opposite points of the equator, then a quarter of it.
        assert_eq!(text(distance(-90.0, 0.0, 90.0, 0.0)), "20020734.0000");
        assert_eq!(text(distance(0.0, 0.0, 90.0, 0.0)), "10010367.0000");
        // The two corners GEOADD accepts that are farthest apart.
        let corners = distance(-180.0, -85.05112878, 180.0, 85.05112878);
        assert_eq!(text(corners), "18919844.7300");
    }

    #[test]
    fn replies_carry_four_fractional_digits_rounded_half_to_even() {
        for (distance, expected) in [
            (0.0, "0.0000"),
            (-0.0, "0.0000"),
            (0.00004, "0.0000"),
            (0.775, "0.7750"),
            (1.0, "1.0000"),
            (166274.15156960033, "166274.1516"),
            (12345678.9, "12345678.9000"),
            (-1.5, "-1.5000"),
            // Exactly representable halves go to the even digit.
            (0.03125, "0.0312"),
            (0.09375, "0.0938"),
            // These scale to an exact half although the doubles are not one,
            // so they also go to the even digit. C's `%.4f` rounds the exact
            // value instead and prints 0.0001, 0.1235 and 0.0003.
            (0.00005, "0.0000"),
            (0.12345, "0.1234"),
            (0.00035, "0.0004"),
        ] {
            assert_eq!(text(distance), expected, "{distance}");
        }
    }
}
