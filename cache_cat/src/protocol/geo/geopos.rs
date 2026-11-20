use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::codec::decode_score;
use crate::protocol::geo::{member_scores, parse_key_members};
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

/// GEOPOS key [member [member ...]]
pub struct GeoPosCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoPosParams {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ReadCommand for GeoPosParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let scores = match member_scores(value, &self.members) {
            Ok(scores) => scores,
            Err(error) => return error.into(),
        };
        let positions = scores
            .into_iter()
            .map(|score| match score {
                Some(score) => {
                    // The centre of the member's cell, not the pair GEOADD got.
                    let (longitude, latitude) = decode_score(score);
                    Value::Array(Some(vec![
                        Value::Double(longitude),
                        Value::Double(latitude),
                    ]))
                }
                // Redis replies with a null array here: `*-1` in RESP2.
                None => Value::Array(None),
            })
            .collect();
        Value::Array(Some(positions))
    }
}

impl Display for GeoPosParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GEOPOS {}", String::from_utf8_lossy(&self.key))?;
        for member in &self.members {
            write!(f, " {}", String::from_utf8_lossy(member))?;
        }
        Ok(())
    }
}

impl GeoPosCommand {
    fn parse(items: &[Value]) -> Result<GeoPosParams, ProtocolError> {
        let (key, members) = parse_key_members(items, "geopos")?;
        Ok(GeoPosParams { key, members })
    }
}

impl ReadRaftCommand for GeoPosCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::GeoPos(Self::parse(items)?))
    }
}

#[async_trait]
impl Command for GeoPosCommand {
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

    // Positions printed by the Redis GEOPOS documentation for these scores.
    const PALERMO: (f64, [&str; 2]) = (
        3479099956230698.0,
        ["13.361389338970184", "38.1155563954963"],
    );
    const CATANIA: (f64, [&str; 2]) = (
        3479447370796909.0,
        ["15.087267458438873", "37.50266842333162"],
    );

    fn params(values: &[&str]) -> GeoPosParams {
        let items = values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect::<Vec<_>>();
        GeoPosCommand::parse(&items).unwrap()
    }

    fn entry(data: ValueObject) -> Option<EntrySnapshot<MyValue>> {
        Some(EntrySnapshot {
            value: MyValue::new(data),
            expire_at: None,
        })
    }

    fn index(members: &[(&str, f64)]) -> Option<EntrySnapshot<MyValue>> {
        let mut zset = SortedSet::new();
        for (member, score) in members {
            zset.zincrby(Bytes::copy_from_slice(member.as_bytes()), *score);
        }
        entry(ValueObject::ZSet(Arc::new(Mutex::new(zset))))
    }

    /// One position on the wire: bulk strings in RESP2, doubles in RESP3.
    fn position(proto: u8, [longitude, latitude]: [&str; 2]) -> String {
        if proto == 3 {
            format!("*2\r\n,{longitude}\r\n,{latitude}\r\n")
        } else {
            format!(
                "*2\r\n${}\r\n{longitude}\r\n${}\r\n{latitude}\r\n",
                longitude.len(),
                latitude.len()
            )
        }
    }

    #[test]
    fn replies_with_one_position_or_null_array_per_member_in_argument_order() {
        let sicily = index(&[("Palermo", PALERMO.0), ("Catania", CATANIA.0)]);
        let reply =
            params(&["GEOPOS", "Sicily", "Catania", "missing", "Palermo"]).execute(sicily.clone());
        for (proto, null) in [(2, "*-1\r\n"), (3, "_\r\n")] {
            assert_eq!(
                String::from_utf8(reply.encode_proto(proto)).unwrap(),
                format!(
                    "*3\r\n{}{null}{}",
                    position(proto, CATANIA.1),
                    position(proto, PALERMO.1)
                )
            );
        }
        // GEOPOS accepts a key without members.
        assert_eq!(
            params(&["GEOPOS", "Sicily"]).execute(sicily).encode(),
            b"*0\r\n"
        );
    }

    #[test]
    fn positions_on_the_limits_are_clamped_and_integers_print_without_a_fraction() {
        // GEOADD stores this score for longitude 180 and the highest latitude.
        let corner = index(&[("corner", (3u64 << 52) as f64)]);
        let reply = params(&["GEOPOS", "k", "corner"]).execute(corner);
        for proto in [2, 3] {
            assert_eq!(
                String::from_utf8(reply.encode_proto(proto)).unwrap(),
                format!("*1\r\n{}", position(proto, ["180", "85.05112878"]))
            );
        }
    }

    #[test]
    fn missing_keys_reply_null_arrays_and_other_types_are_rejected() {
        let reply = params(&["GEOPOS", "missing", "a", "b"]).execute(None);
        assert_eq!(reply.encode_proto(2), b"*2\r\n*-1\r\n*-1\r\n");
        assert_eq!(reply.encode_proto(3), b"*2\r\n_\r\n_\r\n");
        assert_eq!(
            params(&["GEOPOS", "missing"]).execute(None).encode(),
            b"*0\r\n"
        );
        let string = entry(ValueObject::String(Bytes::from_static(b"value")));
        for values in [vec!["GEOPOS", "key"], vec!["GEOPOS", "key", "member"]] {
            assert_eq!(
                params(&values).execute(string.clone()).encode(),
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            );
        }
        assert!(matches!(
            GeoPosCommand::parse(&[Value::BulkString(Some("GEOPOS".into()))]),
            Err(ProtocolError::WrongArgCount("geopos"))
        ));
    }
}
