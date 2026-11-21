//! GEO commands. A geospatial index is an ordinary sorted set whose scores are
//! 52-bit geohashes, so the sorted-set commands work on these keys as well.

mod codec;
pub mod distance;
pub mod geoadd;
pub mod geodist;
pub mod geohash;
pub mod geopos;

use crate::error::ProtocolError;
use crate::mocha::EntrySnapshot;
use crate::protocol::common::float::parse_double;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use bytes::Bytes;
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

/// Arguments of the commands that take `key [member [member ...]]`.
fn parse_key_members(
    items: &[Value],
    command: &'static str,
) -> Result<(Bytes, Vec<Bytes>), ProtocolError> {
    if items.len() < 2 {
        return Err(ProtocolError::WrongArgCount(command));
    }
    let key = items[1]
        .string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("key"))?;
    let members = items[2..]
        .iter()
        .map(|member| {
            member
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("member"))
        })
        .collect::<Result<_, _>>()?;
    Ok((key, members))
}

/// Score of each member in the index at `value`, in argument order. A missing
/// key or member yields `None`; a key of another type is an error.
///
/// The value lock is held for the lookups only, so callers decode and format
/// the scores after it is released.
fn member_scores(
    value: Option<EntrySnapshot<MyValue>>,
    members: &[Bytes],
) -> Result<Vec<Option<f64>>, ProtocolError> {
    match value {
        None => Ok(vec![None; members.len()]),
        Some(entry) => match entry.value.data {
            ZSet(zset) => {
                let zset = zset.lock();
                Ok(members.iter().map(|member| zset.zscore(member)).collect())
            }
            _ => Err(ProtocolError::WrongType),
        },
    }
}
