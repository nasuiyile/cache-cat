//! Port of the Redis `geohash.c` routines that map coordinates to and from the
//! 52-bit sorted-set scores shared by the GEO commands.
//!
//! Only `+ - * /` and integer conversions are used, so every platform derives
//! the same bits from the same input.

/// Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001.
pub(super) const GEO_LONG_MIN: f64 = -180.0;
pub(super) const GEO_LONG_MAX: f64 = 180.0;
pub(super) const GEO_LAT_MIN: f64 = -85.05112878;
pub(super) const GEO_LAT_MAX: f64 = 85.05112878;

/// 26 bits per coordinate: the 52 bits a double score stores exactly.
pub(super) const GEO_STEP_MAX: u8 = 26;
const WGS84_LONG: (f64, f64) = (GEO_LONG_MIN, GEO_LONG_MAX);
const WGS84_LAT: (f64, f64) = (GEO_LAT_MIN, GEO_LAT_MAX);
/// Standard geohashes cover the whole latitude range.
const STANDARD_LAT: (f64, f64) = (-90.0, 90.0);
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Bits of `x` go to the even positions and bits of `y` to the odd ones.
fn interleave64(x: u32, y: u32) -> u64 {
    fn spread(value: u32) -> u64 {
        let mut bits = u64::from(value);
        bits = (bits | (bits << 16)) & 0x0000_FFFF_0000_FFFF;
        bits = (bits | (bits << 8)) & 0x00FF_00FF_00FF_00FF;
        bits = (bits | (bits << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        bits = (bits | (bits << 2)) & 0x3333_3333_3333_3333;
        (bits | (bits << 1)) & 0x5555_5555_5555_5555
    }
    spread(x) | (spread(y) << 1)
}

/// Reverse of `interleave64`: returns the even bits, then the odd bits.
fn deinterleave64(interleaved: u64) -> (u32, u32) {
    fn squash(value: u64) -> u32 {
        let mut bits = value & 0x5555_5555_5555_5555;
        bits = (bits | (bits >> 1)) & 0x3333_3333_3333_3333;
        bits = (bits | (bits >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
        bits = (bits | (bits >> 4)) & 0x00FF_00FF_00FF_00FF;
        bits = (bits | (bits >> 8)) & 0x0000_FFFF_0000_FFFF;
        (bits | (bits >> 16)) as u32
    }
    (squash(interleaved), squash(interleaved >> 1))
}

/// Redis `GeoHashBits`: one cell of the grid that splits each coordinate range
/// into `2^step` parts. The bits of the two cell indices are interleaved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct GeoHashBits {
    pub bits: u64,
    pub step: u8,
}

/// Redis `GeoHashArea`: the edges of a cell, each as `(min, max)` in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct GeoHashArea {
    pub longitude: (f64, f64),
    pub latitude: (f64, f64),
}

impl GeoHashBits {
    /// `geohashEncode` in the ranges of a GEO index: the cell that holds a
    /// position that passed the range check.
    pub(super) fn of(longitude: f64, latitude: f64, step: u8) -> Self {
        GeoHashBits {
            bits: encode(WGS84_LONG, WGS84_LAT, longitude, latitude, step),
            step,
        }
    }

    /// `geohashDecode` in the ranges of a GEO index.
    pub(super) fn area(self) -> GeoHashArea {
        let (lat_bits, long_bits) = deinterleave64(self.bits);
        let scale = (1u64 << self.step) as f64;
        let edges = |bits: u32, (min, max): (f64, f64)| {
            let span = max - min;
            let low = min + (f64::from(bits) / scale) * span;
            // Redis computes the upper edge with uint32_t arithmetic.
            let high = min + (f64::from(bits.wrapping_add(1)) / scale) * span;
            (low, high)
        };
        GeoHashArea {
            longitude: edges(long_bits, WGS84_LONG),
            latitude: edges(lat_bits, WGS84_LAT),
        }
    }

    /// The cell `east` columns and `north` rows away, each -1, 0 or 1, as
    /// `geohashNeighbors` finds it. The grid wraps around at all four edges.
    pub(super) fn neighbor(self, east: i8, north: i8) -> Self {
        let bits = step_coordinate(self.bits, LONG_BITS, self.step, east);
        let bits = step_coordinate(bits, !LONG_BITS, self.step, north);
        GeoHashBits { bits, ..self }
    }
}

/// Longitude indices take the odd bit positions and latitude indices the
/// even ones.
const LONG_BITS: u64 = 0xaaaa_aaaa_aaaa_aaaa;

/// `geohash_move_x` and `geohash_move_y`: moves the index stored in the bits
/// of `mask` by one cell and wraps it around.
fn step_coordinate(bits: u64, mask: u64, step: u8, direction: i8) -> u64 {
    if direction == 0 {
        return bits;
    }
    let unused = 64 - u32::from(step) * 2;
    let moved = bits & mask;
    let kept = bits & !mask;
    // Setting the bits of the other coordinate lets a carry or a borrow
    // travel through them.
    let filler = !mask >> unused;
    let moved = if direction > 0 {
        moved.wrapping_add(filler + 1)
    } else {
        (moved | filler).wrapping_sub(filler + 1)
    };
    (moved & (mask >> unused)) | kept
}

/// `geohashEncode`. Callers pass coordinates inside the ranges.
fn encode(
    long_range: (f64, f64),
    lat_range: (f64, f64),
    longitude: f64,
    latitude: f64,
    step: u8,
) -> u64 {
    let scale = (1u64 << step) as f64;
    let lat_offset = (latitude - lat_range.0) / (lat_range.1 - lat_range.0) * scale;
    let long_offset = (longitude - long_range.0) / (long_range.1 - long_range.0) * scale;
    // Like the C conversion to uint32_t, `as` truncates. A coordinate on the
    // upper limit yields 2^step and therefore sets a bit above the others.
    interleave64(lat_offset as u32, long_offset as u32)
}

/// Score GEOADD stores for a coordinate pair that passed the range check.
///
/// Redis prints the bits as a decimal integer and ZADD parses them back, which
/// rounds like this conversion when longitude 180 sets the 54th bit.
pub(super) fn encode_score(longitude: f64, latitude: f64) -> f64 {
    GeoHashBits::of(longitude, latitude, GEO_STEP_MAX).bits as f64
}

/// `decodeGeohash`: longitude and latitude at the centre of a score's cell.
///
/// Any sorted-set score is accepted, as in Redis. Converting a score outside
/// the u64 range is undefined in C; here it saturates, as it does on ARM64.
pub(super) fn decode_score(score: f64) -> (f64, f64) {
    let cell = GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    };
    let area = cell.area();
    let centre =
        |(low, high): (f64, f64), (min, max): (f64, f64)| ((low + high) / 2.0).clamp(min, max);
    (
        centre(area.longitude, WGS84_LONG),
        centre(area.latitude, WGS84_LAT),
    )
}

/// The standard 11-character geohash of a score, as GEOHASH replies with it.
///
/// Scores use a narrower latitude range than the standard, so the position is
/// decoded and encoded again. 52 bits fill ten characters; Redis always writes
/// '0' as the eleventh.
pub(super) fn geohash_string(score: f64) -> [u8; 11] {
    let (longitude, latitude) = decode_score(score);
    let bits = encode(WGS84_LONG, STANDARD_LAT, longitude, latitude, GEO_STEP_MAX);
    let mut text = [b'0'; 11];
    for (index, byte) in text.iter_mut().take(10).enumerate() {
        *byte = GEOHASH_ALPHABET[(bits >> (52 - (index + 1) * 5)) as usize & 0x1f];
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    const PALERMO: (f64, f64) = (13.361389, 38.115556);
    const CATANIA: (f64, f64) = (15.087269, 37.502669);

    #[test]
    fn redis_documentation_examples_encode_decode_and_hash_identically() {
        // Scores, coordinates and geohashes printed by the Redis GEO documentation.
        for ((longitude, latitude), score, decoded, geohash) in [
            (
                PALERMO,
                3479099956230698.0,
                ("13.361389338970184", "38.1155563954963"),
                "sqc8b49rny0",
            ),
            (
                CATANIA,
                3479447370796909.0,
                ("15.087267458438873", "37.50266842333162"),
                "sqdtr74hyu0",
            ),
        ] {
            assert_eq!(encode_score(longitude, latitude), score);
            let (decoded_longitude, decoded_latitude) = decode_score(score);
            assert_eq!(
                (decoded_longitude.to_string(), decoded_latitude.to_string()),
                (decoded.0.to_string(), decoded.1.to_string())
            );
            assert_eq!(&geohash_string(score), geohash.as_bytes());
        }
    }

    #[test]
    fn range_limits_use_the_bits_redis_produces() {
        assert_eq!(encode_score(GEO_LONG_MIN, GEO_LAT_MIN), 0.0);
        assert_eq!(encode_score(0.0, 0.0), (3u64 << 50) as f64);
        // Both offsets reach 2^26 and set the 53rd and 54th bits.
        assert_eq!(encode_score(GEO_LONG_MAX, GEO_LAT_MAX), (3u64 << 52) as f64);
        assert_eq!(
            decode_score((3u64 << 52) as f64),
            (GEO_LONG_MAX, GEO_LAT_MAX)
        );
        assert_eq!(&geohash_string((3u64 << 50) as f64), b"s0000000000");
    }

    #[test]
    fn cell_centres_encode_back_to_their_own_score() {
        let mut longitude = GEO_LONG_MIN;
        while longitude <= GEO_LONG_MAX {
            let mut latitude = GEO_LAT_MIN;
            while latitude <= GEO_LAT_MAX {
                let score = encode_score(longitude, latitude);
                let (centre_longitude, centre_latitude) = decode_score(score);
                assert_eq!(
                    encode_score(centre_longitude, centre_latitude),
                    score,
                    "{longitude},{latitude}"
                );
                // A cell is at most 360 / 2^26 degrees wide.
                assert!((centre_longitude - longitude).abs() < 1e-5);
                assert!((centre_latitude - latitude).abs() < 1e-5);
                latitude += 4.87;
            }
            longitude += 7.31;
        }
        for (longitude, latitude) in [
            (GEO_LONG_MAX, GEO_LAT_MAX),
            (GEO_LONG_MIN, GEO_LAT_MAX),
            (GEO_LONG_MAX, GEO_LAT_MIN),
        ] {
            let score = encode_score(longitude, latitude);
            let (centre_longitude, centre_latitude) = decode_score(score);
            assert_eq!(encode_score(centre_longitude, centre_latitude), score);
        }
    }

    #[test]
    fn scores_that_are_not_geohashes_decode_inside_the_limits() {
        // GEOHASH also runs on sorted sets filled by ZADD.
        for score in [-1.0, -0.0, 1e300, f64::INFINITY, f64::NEG_INFINITY] {
            let (longitude, latitude) = decode_score(score);
            assert!(
                (GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude),
                "{score}"
            );
            assert!((GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude), "{score}");
            assert!(geohash_string(score).iter().all(u8::is_ascii_alphanumeric));
        }
        assert_eq!(decode_score(-1.0), decode_score(0.0));
    }
}
