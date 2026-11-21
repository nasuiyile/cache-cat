use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::codec::decode_score;
use crate::protocol::geo::distance::{GeoUnit, distance, distance_reply};
use crate::protocol::geo::member_scores;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// GEODIST key member1 member2 [M | KM | FT | MI]
pub struct GeoDistCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoDistParams {
    pub key: Bytes,
    pub members: [Bytes; 2],
    pub unit: GeoUnit,
}

impl ReadCommand for GeoDistParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let scores = match member_scores(value, &self.members) {
            Ok(scores) => scores,
            Err(error) => return error.into(),
        };
        // A missing key or member is a null, not an error.
        let [Some(score1), Some(score2)] = scores[..] else {
            return Value::Null;
        };
        // Measured between the centres of the two cells.
        let (longitude1, latitude1) = decode_score(score1);
        let (longitude2, latitude2) = decode_score(score2);
        let meters = distance(longitude1, latitude1, longitude2, latitude2);
        distance_reply(meters / self.unit.to_meters())
    }
}

impl Display for GeoDistParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GEODIST {}", String::from_utf8_lossy(&self.key))?;
        for member in &self.members {
            write!(f, " {}", String::from_utf8_lossy(member))?;
        }
        write!(f, " {}", self.unit)
    }
}

impl GeoDistCommand {
    fn parse(items: &[Value]) -> Result<GeoDistParams, ProtocolError> {
        // Redis rejects the unit before it looks at the key.
        let unit = match items.len() {
            ..4 => return Err(ProtocolError::WrongArgCount("geodist")),
            4 => GeoUnit::Meters,
            5 => GeoUnit::parse(&items[4])?,
            _ => return Err(ProtocolError::SyntaxError),
        };
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let member = |item: &Value| {
            item.string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("member"))
        };
        Ok(GeoDistParams {
            key,
            members: [member(&items[2])?, member(&items[3])?],
            unit,
        })
    }
}

impl ReadRaftCommand for GeoDistCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::GeoDist(Self::parse(items)?))
    }
}

#[async_trait]
impl Command for GeoDistCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use crate::raft::types::core::value_object::ValueObject;
    use parking_lot::Mutex;
    use std::sync::Arc;

    // Scores GEOADD stores for the two cities of the Redis documentation.
    const PALERMO: f64 = 3479099956230698.0;
    const CATANIA: f64 = 3479447370796909.0;

    fn items(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    fn params(values: &[&str]) -> GeoDistParams {
        GeoDistCommand::parse(&items(values)).unwrap()
    }

    fn entry(data: ValueObject) -> Option<EntrySnapshot<MyValue>> {
        Some(EntrySnapshot {
            value: MyValue::new(data),
            expire_at: None,
        })
    }

    fn sicily() -> Option<EntrySnapshot<MyValue>> {
        let mut zset = SortedSet::new();
        zset.zincrby("Palermo".into(), PALERMO);
        zset.zincrby("Catania".into(), CATANIA);
        entry(ValueObject::ZSet(Arc::new(Mutex::new(zset))))
    }

    #[test]
    fn replies_with_the_distance_as_a_bulk_string_in_both_protocols() {
        // The replies of the Redis documentation example.
        for (unit, expected) in [
            (None, "166274.1516"),
            (Some("m"), "166274.1516"),
            (Some("km"), "166.2742"),
            (Some("KM"), "166.2742"),
            (Some("mi"), "103.3182"),
            (Some("ft"), "545518.8700"),
        ] {
            let mut values = vec!["GEODIST", "Sicily", "Palermo", "Catania"];
            values.extend(unit);
            let reply = params(&values).execute(sicily());
            let bulk = format!("${}\r\n{expected}\r\n", expected.len());
            for proto in [2, 3] {
                assert_eq!(
                    String::from_utf8(reply.encode_proto(proto)).unwrap(),
                    bulk,
                    "{values:?}"
                );
            }
        }
        // The order of the members does not matter, and a member is at
        // distance zero from itself.
        assert_eq!(
            params(&["GEODIST", "Sicily", "Catania", "Palermo"])
                .execute(sicily())
                .encode(),
            b"$11\r\n166274.1516\r\n"
        );
        assert_eq!(
            params(&["GEODIST", "Sicily", "Palermo", "Palermo", "mi"])
                .execute(sicily())
                .encode(),
            b"$6\r\n0.0000\r\n"
        );
    }

    #[test]
    fn missing_keys_and_members_reply_null_and_other_types_are_rejected() {
        for (values, value) in [
            (["GEODIST", "Sicily", "Palermo", "missing"], sicily()),
            (["GEODIST", "Sicily", "missing", "Catania"], sicily()),
            (["GEODIST", "Sicily", "missing", "absent"], sicily()),
            (["GEODIST", "missing", "Palermo", "Catania"], None),
        ] {
            let reply = params(&values).execute(value);
            assert_eq!(reply.encode_proto(2), b"$-1\r\n", "{values:?}");
            assert_eq!(reply.encode_proto(3), b"_\r\n", "{values:?}");
        }
        let string = entry(ValueObject::String(Bytes::from_static(b"value")));
        assert_eq!(
            params(&["GEODIST", "key", "a", "b"])
                .execute(string)
                .encode(),
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
        );
    }

    #[test]
    fn arguments_are_checked_like_redis() {
        for values in [
            &["GEODIST"][..],
            &["GEODIST", "key"],
            &["GEODIST", "key", "a"],
        ] {
            assert!(matches!(
                GeoDistCommand::parse(&items(values)),
                Err(ProtocolError::WrongArgCount("geodist"))
            ));
        }
        // Anything after the unit is a syntax error, whatever the unit says.
        for values in [
            &["GEODIST", "key", "a", "b", "km", "extra"][..],
            &["GEODIST", "key", "a", "b", "bad", "extra"],
        ] {
            assert!(matches!(
                GeoDistCommand::parse(&items(values)),
                Err(ProtocolError::SyntaxError)
            ));
        }
        assert_eq!(
            GeoDistCommand::parse(&items(&["GEODIST", "key", "a", "b", "yards"]))
                .unwrap_err()
                .to_string(),
            "ERR unsupported unit provided. please use M, KM, FT, MI"
        );

        let parsed = params(&["GEODIST", "key", "a", "b", "Ft"]);
        assert_eq!(parsed.unit, GeoUnit::Feet);
        assert_eq!(parsed.to_string(), "GEODIST key a b ft");
        assert_eq!(params(&["GEODIST", "key", "a", "b"]).unit, GeoUnit::Meters);
    }
}
