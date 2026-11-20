use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::codec::geohash_string;
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

/// GEOHASH key [member [member ...]]
pub struct GeoHashCommand;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoHashParams {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl ReadCommand for GeoHashParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let scores = match member_scores(value, &self.members) {
            Ok(scores) => scores,
            Err(error) => return error.into(),
        };
        let hashes = scores
            .into_iter()
            .map(|score| match score {
                Some(score) => {
                    Value::BulkString(Some(Bytes::copy_from_slice(&geohash_string(score))))
                }
                // A missing key or member is a null, not an error.
                None => Value::Null,
            })
            .collect();
        Value::Array(Some(hashes))
    }
}

impl Display for GeoHashParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GEOHASH {}", String::from_utf8_lossy(&self.key))?;
        for member in &self.members {
            write!(f, " {}", String::from_utf8_lossy(member))?;
        }
        Ok(())
    }
}

impl GeoHashCommand {
    fn parse(items: &[Value]) -> Result<GeoHashParams, ProtocolError> {
        let (key, members) = parse_key_members(items, "geohash")?;
        Ok(GeoHashParams { key, members })
    }
}

impl ReadRaftCommand for GeoHashCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::GeoHash(Self::parse(items)?))
    }
}

#[async_trait]
impl Command for GeoHashCommand {
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

    fn params(values: &[&str]) -> GeoHashParams {
        let items = values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect::<Vec<_>>();
        GeoHashCommand::parse(&items).unwrap()
    }

    fn entry(data: ValueObject) -> Option<EntrySnapshot<MyValue>> {
        Some(EntrySnapshot {
            value: MyValue::new(data),
            expire_at: None,
        })
    }

    #[test]
    fn replies_with_one_hash_or_null_per_member_in_argument_order() {
        let mut zset = SortedSet::new();
        zset.zincrby("Palermo".into(), 3479099956230698.0);
        zset.zincrby("Catania".into(), 3479447370796909.0);
        let index = entry(ValueObject::ZSet(Arc::new(Mutex::new(zset))));

        let reply =
            params(&["GEOHASH", "Sicily", "Catania", "missing", "Palermo"]).execute(index.clone());
        assert_eq!(
            reply.encode_proto(2),
            b"*3\r\n$11\r\nsqdtr74hyu0\r\n$-1\r\n$11\r\nsqc8b49rny0\r\n"
        );
        assert_eq!(
            reply.encode_proto(3),
            b"*3\r\n$11\r\nsqdtr74hyu0\r\n_\r\n$11\r\nsqc8b49rny0\r\n"
        );
        // GEOHASH accepts a key without members.
        assert_eq!(
            params(&["GEOHASH", "Sicily"]).execute(index).encode(),
            b"*0\r\n"
        );
    }

    #[test]
    fn missing_keys_reply_nulls_and_other_types_are_rejected() {
        assert_eq!(
            params(&["GEOHASH", "missing", "a", "b"])
                .execute(None)
                .encode_proto(2),
            b"*2\r\n$-1\r\n$-1\r\n"
        );
        assert_eq!(
            params(&["GEOHASH", "missing"]).execute(None).encode(),
            b"*0\r\n"
        );
        let string = entry(ValueObject::String(Bytes::from_static(b"value")));
        for values in [vec!["GEOHASH", "key"], vec!["GEOHASH", "key", "member"]] {
            assert_eq!(
                params(&values).execute(string.clone()).encode(),
                b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            );
        }
        assert!(matches!(
            GeoHashCommand::parse(&[Value::BulkString(Some("GEOHASH".into()))]),
            Err(ProtocolError::WrongArgCount("geohash"))
        ));
    }
}
