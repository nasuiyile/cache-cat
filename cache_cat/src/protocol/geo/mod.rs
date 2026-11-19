//! GEO commands. A geospatial index is an ordinary sorted set whose scores are
//! 52-bit geohashes, so the sorted-set commands work on these keys as well.

mod codec;
pub mod geoadd;
pub mod geohash;

use crate::error::ProtocolError;
use crate::protocol::common::float::parse_double;
use crate::raft::types::core::response_value::Value;
use codec::{GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN};

/// Redis `extractLongLatOrReply`: parse both numbers, then check their ranges.
fn parse_long_lat(longitude: &Value, latitude: &Value) -> Result<(f64, f64), ProtocolError> {
    let parse = |value: &Value| {
        value
            .string_bytes_clone()
            .and_then(|bytes| parse_double(&bytes))
            .ok_or(ProtocolError::response("ERR value is not a valid float"))
    };
    let longitude = parse(longitude)?;
    let latitude = parse(latitude)?;
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        // Redis formats both values with C's `%f`.
        return Err(ProtocolError::response(format!(
            "ERR invalid longitude,latitude pair {longitude:.6},{latitude:.6}"
        )));
    }
    Ok((longitude, latitude))
}
