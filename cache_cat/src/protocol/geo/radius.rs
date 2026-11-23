//! What GEORADIUS and GEORADIUSBYMEMBER share besides the search itself: the
//! arguments after the centre, the reply, and the write of STORE. Redis runs
//! both commands through `georadiusGeneric`.

use crate::error::ProtocolError;
use crate::mocha::{ExpirePolicy, MochaOperation};
use crate::protocol::common::float::parse_double;
use crate::protocol::geo::distance::{GeoUnit, distance_reply};
use crate::protocol::geo::search::{GeoOrder, GeoPoint, RadiusSearch};
use crate::protocol::option_eq;
use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::mocha::cas::ComputedWrite;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::sorted_set::SortedSet;
use crate::raft::types::core::value_object::ValueObject;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Redis `extractDistanceOrReply`: the radius, then its unit.
pub(super) fn parse_radius(radius: &Value, unit: &Value) -> Result<(f64, GeoUnit), ProtocolError> {
    let radius = radius
        .string_bytes_clone()
        .and_then(|bytes| parse_double(&bytes))
        .ok_or(ProtocolError::response("ERR need numeric radius"))?;
    if radius < 0.0 {
        return Err(ProtocolError::response("ERR radius cannot be negative"));
    }
    Ok((radius, GeoUnit::parse(unit)?))
}

/// The options that follow the radius and its unit, except the destination
/// of STORE.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadiusOptions {
    pub with_dist: bool,
    pub with_hash: bool,
    pub with_coord: bool,
    /// `COUNT`: the most members to return.
    pub count: Option<u64>,
    /// `ANY`: stop at the first `count` members found.
    pub any: bool,
    pub order: Option<GeoOrder>,
}

impl RadiusOptions {
    /// The options, and the destination key with whether STOREDIST named it.
    ///
    /// Redis reads the options from left to right and replies with the first
    /// error it meets. The two combinations it rejects are checked last.
    pub(super) fn parse(options: &[Value]) -> Result<(Self, Option<(Bytes, bool)>), ProtocolError> {
        let mut parsed = RadiusOptions {
            with_dist: false,
            with_hash: false,
            with_coord: false,
            count: None,
            any: false,
            order: None,
        };
        let mut store = None;
        let mut index = 0;
        while index < options.len() {
            let option = options[index].string_bytes_clone().unwrap_or_default();
            // An option that takes a value is a syntax error as the last
            // argument. A repeated option replaces the earlier one.
            let value = options.get(index + 1);
            if option_eq(&option, b"WITHDIST") {
                parsed.with_dist = true;
            } else if option_eq(&option, b"WITHHASH") {
                parsed.with_hash = true;
            } else if option_eq(&option, b"WITHCOORD") {
                parsed.with_coord = true;
            } else if option_eq(&option, b"ANY") {
                parsed.any = true;
            } else if option_eq(&option, b"ASC") {
                parsed.order = Some(GeoOrder::Asc);
            } else if option_eq(&option, b"DESC") {
                parsed.order = Some(GeoOrder::Desc);
            } else if let Some(value) = value
                && option_eq(&option, b"COUNT")
            {
                let number = value.try_parse_canonical_i64()?;
                if number <= 0 {
                    return Err(ProtocolError::response("ERR COUNT must be > 0"));
                }
                parsed.count = Some(number as u64);
                index += 1;
            } else if let Some(value) = value
                && (option_eq(&option, b"STORE") || option_eq(&option, b"STOREDIST"))
            {
                let destination = value
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("key"))?;
                store = Some((destination, option_eq(&option, b"STOREDIST")));
                index += 1;
            } else {
                return Err(ProtocolError::SyntaxError);
            }
            index += 1;
        }

        if store.is_some() && (parsed.with_dist || parsed.with_hash || parsed.with_coord) {
            return Err(ProtocolError::response(
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ));
        }
        if parsed.any && parsed.count.is_none() {
            return Err(ProtocolError::response(
                "ERR the ANY argument requires COUNT argument",
            ));
        }
        Ok((parsed, store))
    }

    /// The search these options ask for around a position.
    pub(super) fn search(
        &self,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> RadiusSearch {
        RadiusSearch {
            longitude,
            latitude,
            radius,
            unit,
            count: self.count,
            any: self.any,
            order: self.order,
        }
    }

    pub(super) fn reply(&self, points: Vec<GeoPoint>, unit: GeoUnit) -> Value {
        members_reply(
            points,
            unit,
            self.with_dist,
            self.with_hash,
            self.with_coord,
        )
    }
}

impl fmt::Display for RadiusOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(count) = self.count {
            write!(f, " COUNT {count}")?;
        }
        for (enabled, option) in [
            (self.any, " ANY"),
            (self.order == Some(GeoOrder::Asc), " ASC"),
            (self.order == Some(GeoOrder::Desc), " DESC"),
            (self.with_coord, " WITHCOORD"),
            (self.with_dist, " WITHDIST"),
            (self.with_hash, " WITHHASH"),
        ] {
            if enabled {
                f.write_str(option)?;
            }
        }
        Ok(())
    }
}

/// One entry per member: its name, or an array that starts with the name
/// when a WITH option adds to it. Distances are in `unit`.
pub(super) fn members_reply(
    points: Vec<GeoPoint>,
    unit: GeoUnit,
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
) -> Value {
    let to_meters = unit.to_meters();
    let extras = usize::from(with_dist) + usize::from(with_hash) + usize::from(with_coord);
    let entries = points
        .into_iter()
        .map(|point| {
            let member = Value::BulkString(Some(point.member));
            if extras == 0 {
                return member;
            }
            // Redis sends the extras in this order, whatever the order of
            // the options.
            let mut entry = Vec::with_capacity(extras + 1);
            entry.push(member);
            if with_dist {
                entry.push(distance_reply(point.distance / to_meters));
            }
            if with_hash {
                entry.push(Value::Integer(point.score as i64));
            }
            if with_coord {
                entry.push(Value::Array(Some(vec![
                    Value::Double(point.longitude),
                    Value::Double(point.latitude),
                ])));
            }
            Value::Array(Some(entry))
        })
        .collect();
    Value::Array(Some(entries))
}

/// STORE and STOREDIST: the write that replaces `destination` with a sorted
/// set of the members found, and the number of members as the reply.
///
/// With STOREDIST the score of a member is its distance in `unit` instead of
/// its geohash. The destination is replaced whatever it held and loses its
/// TTL. Without a member to store it is deleted.
pub(super) fn store(
    destination: Bytes,
    store_dist: bool,
    unit: GeoUnit,
    points: Vec<GeoPoint>,
) -> (Vec<ComputedWrite>, Value) {
    let to_meters = unit.to_meters();
    let members = points
        .into_iter()
        .map(|point| {
            let score = if store_dist {
                point.distance / to_meters
            } else {
                point.score
            };
            (point.member, score)
        })
        // A sorted set cannot hold a NaN. Redis asserts on one here.
        .filter(|(_, score)| !score.is_nan())
        .collect();

    let mut stored = SortedSet::new();
    let added = stored.zadd(ZAddReq {
        key: destination.clone(),
        nx: false,
        xx: false,
        gt: false,
        lt: false,
        ch: false,
        members,
    });
    let operation = if stored.is_empty() {
        MochaOperation::Remove
    } else {
        MochaOperation::Insert {
            value: MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(stored)))),
            expire: ExpirePolicy::Persistent,
        }
    };
    (
        vec![ComputedWrite {
            key: destination,
            operation,
        }],
        Value::Integer(added),
    )
}
