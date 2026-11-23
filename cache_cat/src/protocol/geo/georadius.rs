use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::radius::{RadiusOptions, members_reply, parse_radius, store};
use crate::protocol::geo::search::{GeoPoint, RadiusSearch};
use crate::protocol::geo::{parse_long_lat, sorted_set};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::{ComputedWrite, MultiReadComputeCommand};
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// GEORADIUS key longitude latitude radius <M | KM | FT | MI> [WITHCOORD]
/// [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
/// [STORE key | STOREDIST key]
///
/// Without STORE or STOREDIST the command only reads, so it takes the read
/// path like the other queries. With one of them it is a replicated write.
pub struct GeoRadiusCommand;

/// GEORADIUS that replies with the members it finds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadiusParams {
    pub key: Bytes,
    /// `Err` holds the error reply for the arguments after the key. Redis
    /// checks the type of the key before it reads them, so the error is
    /// decided while parsing but sent only once the key turns out not to
    /// hold another type.
    pub query: Result<GeoRadiusQuery, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadiusQuery {
    pub search: RadiusSearch,
    pub with_dist: bool,
    pub with_hash: bool,
    pub with_coord: bool,
}

impl GeoRadiusQuery {
    fn reply(&self, points: Vec<GeoPoint>) -> Value {
        members_reply(
            points,
            self.search.unit,
            self.with_dist,
            self.with_hash,
            self.with_coord,
        )
    }
}

impl ReadCommand for GeoRadiusParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let zset = match sorted_set(value) {
            Ok(zset) => zset,
            Err(error) => return error.into(),
        };
        let query = match &self.query {
            Ok(query) => query,
            Err(message) => return Value::error(message.clone()),
        };
        match zset {
            Some(zset) => query.reply(query.search.points(&zset)),
            None => Value::Array(Some(Vec::new())),
        }
    }
}

impl Display for GeoRadiusParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GEORADIUS {}", String::from_utf8_lossy(&self.key))?;
        let query = match &self.query {
            Ok(query) => query,
            Err(message) => return write!(f, " (rejected: {message})"),
        };
        write!(f, " {}", query.search)?;
        for (enabled, option) in [
            (query.with_coord, " WITHCOORD"),
            (query.with_dist, " WITHDIST"),
            (query.with_hash, " WITHHASH"),
        ] {
            if enabled {
                f.write_str(option)?;
            }
        }
        Ok(())
    }
}

/// GEORADIUS with STORE or STOREDIST, which writes the members it finds to
/// another key as a sorted set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadiusStoreReq {
    pub key: Bytes,
    pub search: RadiusSearch,
    pub destination: Bytes,
    /// STOREDIST: the score of a member is its distance in the unit of the
    /// radius instead of its geohash.
    pub store_dist: bool,
}

impl Display for GeoRadiusStoreReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GEORADIUS {} {} {} {}",
            String::from_utf8_lossy(&self.key),
            self.search,
            if self.store_dist {
                "STOREDIST"
            } else {
                "STORE"
            },
            String::from_utf8_lossy(&self.destination)
        )
    }
}

impl MultiReadComputeCommand for GeoRadiusStoreReq {
    fn read_keys(&self) -> impl Iterator<Item = &Bytes> {
        std::iter::once(&self.key)
    }

    fn mutate_writes(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (Vec<ComputedWrite>, Value) {
        let source = match sorted_set(read_entries.into_iter().next().flatten()) {
            Ok(source) => source,
            Err(error) => return (Vec::new(), error.into()),
        };
        let points = source.map_or_else(Vec::new, |zset| self.search.points(&zset));
        store(self.destination, self.store_dist, self.search.unit, points)
    }
}

/// The arguments after the key.
struct Arguments {
    search: RadiusSearch,
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
    /// Destination key, and whether STOREDIST named it.
    store: Option<(Bytes, bool)>,
}

impl GeoRadiusCommand {
    /// Redis `georadiusGeneric` reads the arguments in this order and replies
    /// with the first error it meets.
    fn parse_arguments(items: &[Value]) -> Result<Arguments, ProtocolError> {
        let (longitude, latitude) = parse_long_lat(&items[2], &items[3])?;
        let (radius, unit) = parse_radius(&items[4], &items[5])?;
        let (options, store) = RadiusOptions::parse(&items[6..])?;
        Ok(Arguments {
            search: options.search(longitude, latitude, radius, unit),
            with_dist: options.with_dist,
            with_hash: options.with_hash,
            with_coord: options.with_coord,
            store,
        })
    }
}

impl RaftCommand for GeoRadiusCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        // Only the arity is checked before the key is looked up.
        if items.len() < 6 {
            return Err(ProtocolError::WrongArgCount("georadius"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let query = match Self::parse_arguments(items) {
            Ok(Arguments {
                search,
                store: Some((destination, store_dist)),
                ..
            }) => {
                return Ok(Operation::Redis(RedisOperation::RedisGeoRadiusStore(
                    GeoRadiusStoreReq {
                        key,
                        search,
                        destination,
                        store_dist,
                    },
                )));
            }
            Ok(arguments) => Ok(GeoRadiusQuery {
                search: arguments.search,
                with_dist: arguments.with_dist,
                with_hash: arguments.with_hash,
                with_coord: arguments.with_coord,
            }),
            // Nothing is written, so the read path reports the error.
            Err(error) => Err(error.to_string()),
        };
        Ok(Operation::Read(ReadOperation::GeoRadius(GeoRadiusParams {
            key,
            query,
        })))
    }
}

#[async_trait]
impl Command for GeoRadiusCommand {
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

        match self.raft_request(items)? {
            Operation::Read(read) => server.app.read(read, client.db_number).await,
            write => server.app.write(write, client.db_number).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::{ExpirePolicy, MochaOperation};
    use crate::protocol::geo::distance::GeoUnit;
    use crate::protocol::geo::search::GeoOrder;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use crate::raft::types::core::value_object::ValueObject;
    use parking_lot::Mutex;
    use std::sync::Arc;

    // Scores GEOADD stores for the two cities of the Redis documentation.
    const PALERMO: f64 = 3479099956230698.0;
    const CATANIA: f64 = 3479447370796909.0;
    const WRONG_TYPE: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    fn items(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    fn request(options: &[&str]) -> Operation {
        let mut values = vec!["GEORADIUS", "Sicily", "15", "37", "200", "km"];
        values.extend(options);
        GeoRadiusCommand.raft_request(&items(&values)).unwrap()
    }

    fn params(options: &[&str]) -> GeoRadiusParams {
        match request(options) {
            Operation::Read(ReadOperation::GeoRadius(params)) => params,
            other => panic!("{options:?} is not a read: {other:?}"),
        }
    }

    fn store(options: &[&str]) -> GeoRadiusStoreReq {
        match request(options) {
            Operation::Redis(RedisOperation::RedisGeoRadiusStore(request)) => request,
            other => panic!("{options:?} is not a store: {other:?}"),
        }
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

    fn string() -> Option<EntrySnapshot<MyValue>> {
        entry(ValueObject::String(Bytes::from_static(b"value")))
    }

    fn reply(options: &[&str], proto: u8) -> String {
        String::from_utf8(params(options).execute(sicily()).encode_proto(proto)).unwrap()
    }

    /// The error reply for arguments given in place of `15 37 200 km`.
    fn rejected(arguments: &[&str]) -> String {
        let mut values = vec!["GEORADIUS", "Sicily"];
        values.extend(arguments);
        let operation = GeoRadiusCommand.raft_request(&items(&values)).unwrap();
        let Operation::Read(ReadOperation::GeoRadius(params)) = operation else {
            panic!("{arguments:?} was accepted as a write");
        };
        // The same reply whether or not the key exists.
        let reply = params.execute(sicily()).encode();
        assert_eq!(params.execute(None).encode(), reply, "{arguments:?}");
        // A key of another type is reported instead.
        assert_eq!(
            params.execute(string()).encode(),
            WRONG_TYPE,
            "{arguments:?}"
        );
        String::from_utf8(reply).unwrap()
    }

    #[test]
    fn replies_with_the_members_of_the_redis_documentation_example() {
        assert_eq!(reply(&[], 2), "*2\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n");
        assert_eq!(reply(&[], 3), reply(&[], 2));
        assert_eq!(
            reply(&["WITHDIST"], 2),
            "*2\r\n*2\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n*2\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n"
        );
        assert_eq!(reply(&["WITHDIST"], 3), reply(&["WITHDIST"], 2));

        // Coordinates are bulk strings in RESP2 and doubles in RESP3.
        assert_eq!(
            reply(&["WITHCOORD"], 2),
            "*2\r\n\
             *2\r\n$7\r\nPalermo\r\n*2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n\
             *2\r\n$7\r\nCatania\r\n*2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n"
        );
        assert_eq!(
            reply(&["WITHCOORD"], 3),
            "*2\r\n\
             *2\r\n$7\r\nPalermo\r\n*2\r\n,13.361389338970184\r\n,38.1155563954963\r\n\
             *2\r\n$7\r\nCatania\r\n*2\r\n,15.087267458438873\r\n,37.50266842333162\r\n"
        );

        // Distance, hash and coordinates come in that order, however the
        // options are written.
        let everything = "*2\r\n\
             *4\r\n$7\r\nPalermo\r\n$8\r\n190.4424\r\n:3479099956230698\r\n\
             *2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n\
             *4\r\n$7\r\nCatania\r\n$7\r\n56.4413\r\n:3479447370796909\r\n\
             *2\r\n$18\r\n15.087267458438873\r\n$17\r\n37.50266842333162\r\n";
        assert_eq!(reply(&["WITHCOORD", "WITHHASH", "WITHDIST"], 2), everything);
        assert_eq!(reply(&["withdist", "WithCoord", "WITHHASH"], 2), everything);
    }

    #[test]
    fn count_and_order_select_the_members() {
        let names = |options: &[&str]| {
            let Value::Array(Some(entries)) = params(options).execute(sicily()) else {
                panic!("{options:?}");
            };
            entries
                .into_iter()
                .map(|entry| match entry {
                    Value::BulkString(Some(member)) => String::from_utf8(member.to_vec()).unwrap(),
                    other => panic!("{other:?}"),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&["ASC"]), ["Catania", "Palermo"]);
        assert_eq!(names(&["DESC"]), ["Palermo", "Catania"]);
        // COUNT alone returns the nearest members.
        assert_eq!(names(&["COUNT", "1"]), ["Catania"]);
        assert_eq!(names(&["COUNT", "1", "DESC"]), ["Palermo"]);
        assert_eq!(names(&["COUNT", "5"]), ["Catania", "Palermo"]);
        // ANY returns the first ones the scan finds.
        assert_eq!(names(&["COUNT", "1", "ANY"]), ["Palermo"]);
        assert_eq!(names(&["ANY", "COUNT", "2", "ASC"]), ["Catania", "Palermo"]);
        // The last of a repeated option counts.
        assert_eq!(
            names(&["COUNT", "2", "DESC", "ASC", "COUNT", "1"]),
            ["Catania"]
        );

        // The unit of the radius is the unit of the distances.
        assert_eq!(
            String::from_utf8(
                GeoRadiusCommand
                    .raft_request(&items(&[
                        "GEORADIUS",
                        "Sicily",
                        "15",
                        "37",
                        "100",
                        "MI",
                        "WITHDIST"
                    ]))
                    .map(|operation| match operation {
                        Operation::Read(ReadOperation::GeoRadius(params)) => params,
                        other => panic!("{other:?}"),
                    })
                    .unwrap()
                    .execute(sicily())
                    .encode()
            )
            .unwrap(),
            "*1\r\n*2\r\n$7\r\nCatania\r\n$7\r\n35.0711\r\n"
        );
    }

    #[test]
    fn missing_keys_and_empty_results_reply_with_an_empty_array() {
        for proto in [2, 3] {
            assert_eq!(params(&[]).execute(None).encode_proto(proto), b"*0\r\n");
            assert_eq!(
                params(&["WITHDIST", "COUNT", "3"])
                    .execute(None)
                    .encode_proto(proto),
                b"*0\r\n"
            );
        }
        let far = GeoRadiusCommand
            .raft_request(&items(&["GEORADIUS", "Sicily", "-120", "40", "10", "km"]))
            .unwrap();
        let Operation::Read(ReadOperation::GeoRadius(far)) = far else {
            panic!("expected a read");
        };
        assert_eq!(far.execute(sicily()).encode(), b"*0\r\n");
        assert_eq!(params(&[]).execute(string()).encode(), WRONG_TYPE);
    }

    #[test]
    fn only_the_arity_is_checked_before_the_key() {
        for values in [
            &["GEORADIUS"][..],
            &["GEORADIUS", "key"],
            &["GEORADIUS", "key", "15", "37", "200"],
            // The arity counts arguments without looking at them.
            &["GEORADIUS", "key", "bad", "bad", "bad"],
        ] {
            assert!(matches!(
                GeoRadiusCommand.raft_request(&items(values)),
                Err(ProtocolError::WrongArgCount("georadius"))
            ));
        }
    }

    #[test]
    fn argument_errors_follow_the_order_redis_reads_them_in() {
        for (arguments, error) in [
            (
                &["x", "37", "200", "km"][..],
                "ERR value is not a valid float",
            ),
            (
                &["15", "nan", "200", "km"],
                "ERR value is not a valid float",
            ),
            (
                &["181", "37", "200", "km"],
                "ERR invalid longitude,latitude pair 181.000000,37.000000",
            ),
            (
                &["15", "-86", "200", "km"],
                "ERR invalid longitude,latitude pair 15.000000,-86.000000",
            ),
            // The position is checked before the radius, the radius before
            // its unit, and the unit before the options.
            (&["x", "37", "far", "km"], "ERR value is not a valid float"),
            (&["15", "37", "far", "km"], "ERR need numeric radius"),
            (&["15", "37", "", "km"], "ERR need numeric radius"),
            (&["15", "37", "nan", "yards"], "ERR need numeric radius"),
            (
                &["15", "37", "-1", "yards"],
                "ERR radius cannot be negative",
            ),
            (&["15", "37", "-0.5", "km"], "ERR radius cannot be negative"),
            (
                &["15", "37", "200", "yards"],
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ),
            (
                &["15", "37", "200", "yards", "BOGUS"],
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ),
            (&["15", "37", "200", "km", "BOGUS"], "ERR syntax error"),
            (
                &["15", "37", "200", "km", "WITHDIST", "extra"],
                "ERR syntax error",
            ),
            // An option without its value is a syntax error.
            (&["15", "37", "200", "km", "COUNT"], "ERR syntax error"),
            (&["15", "37", "200", "km", "STORE"], "ERR syntax error"),
            (
                &["15", "37", "200", "km", "ASC", "STOREDIST"],
                "ERR syntax error",
            ),
            (
                &["15", "37", "200", "km", "COUNT", "many"],
                "ERR value is not an integer or out of range",
            ),
            (
                &["15", "37", "200", "km", "COUNT", "1.5"],
                "ERR value is not an integer or out of range",
            ),
            (
                &["15", "37", "200", "km", "COUNT", "9223372036854775808"],
                "ERR value is not an integer or out of range",
            ),
            (
                &["15", "37", "200", "km", "COUNT", "0"],
                "ERR COUNT must be > 0",
            ),
            (
                &["15", "37", "200", "km", "COUNT", "-3"],
                "ERR COUNT must be > 0",
            ),
            // Options are read left to right, so the first bad one decides.
            (
                &["15", "37", "200", "km", "COUNT", "0", "BOGUS"],
                "ERR COUNT must be > 0",
            ),
            (
                &["15", "37", "200", "km", "BOGUS", "COUNT", "0"],
                "ERR syntax error",
            ),
            // These two are checked after every option was read.
            (
                &["15", "37", "200", "km", "ANY"],
                "ERR the ANY argument requires COUNT argument",
            ),
            (
                &["15", "37", "200", "km", "ANY", "BOGUS"],
                "ERR syntax error",
            ),
            (
                &["15", "37", "200", "km", "STORE", "dst", "WITHDIST"],
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ),
            (
                &[
                    "15",
                    "37",
                    "200",
                    "km",
                    "WITHCOORD",
                    "STOREDIST",
                    "dst",
                    "ANY",
                ],
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ),
            (
                &["15", "37", "200", "km", "STORE", "dst", "ANY"],
                "ERR the ANY argument requires COUNT argument",
            ),
        ] {
            assert_eq!(
                rejected(arguments),
                format!("-{error}\r\n"),
                "{arguments:?}"
            );
        }
    }

    #[test]
    fn options_are_matched_like_strcasecmp_and_values_are_not_options() {
        let parsed = params(&["withDIST\0x", "Count", "7", "any", "desc"]);
        let query = parsed.query.as_ref().unwrap();
        assert!(query.with_dist && !query.with_hash && !query.with_coord);
        assert_eq!(query.search.count, Some(7));
        assert!(query.search.any);
        assert_eq!(query.search.order, Some(GeoOrder::Desc));
        assert_eq!(query.search.unit, GeoUnit::Kilometers);
        assert_eq!(
            parsed.to_string(),
            "GEORADIUS Sicily 15 37 200 km COUNT 7 ANY DESC WITHDIST"
        );
        assert_eq!(
            params(&["BOGUS"]).to_string(),
            "GEORADIUS Sicily (rejected: ERR syntax error)"
        );

        // A destination may be named like an option, and the last STORE or
        // STOREDIST decides both the key and the scores.
        let request = store(&["STORE", "COUNT", "storedist", "ASC", "COUNT", "2"]);
        assert_eq!(request.destination.as_ref(), b"ASC");
        assert!(request.store_dist);
        assert_eq!(request.search.count, Some(2));
        assert_eq!(request.search.order, None);
        assert_eq!(
            request.to_string(),
            "GEORADIUS Sicily 15 37 200 km COUNT 2 STOREDIST ASC"
        );
        let request = store(&["STOREDIST", "a", "STORE", "b"]);
        assert_eq!(request.destination.as_ref(), b"b");
        assert!(!request.store_dist);
        // An infinite radius is a number, and zero is not negative.
        let radius = |text: &str| {
            let values = items(&["GEORADIUS", "key", "15", "37", text, "m"]);
            let arguments = GeoRadiusCommand::parse_arguments(&values);
            arguments.ok().map(|arguments| arguments.search.radius)
        };
        assert_eq!(radius("inf"), Some(f64::INFINITY));
        assert_eq!(radius("-0"), Some(0.0));
        assert_eq!(radius("0x10"), Some(16.0));
    }

    fn stored(
        request: GeoRadiusStoreReq,
        source: Option<EntrySnapshot<MyValue>>,
    ) -> (Vec<ComputedWrite>, Value) {
        assert_eq!(request.read_keys().collect::<Vec<_>>(), [&request.key]);
        request.mutate_writes(vec![source], 1)
    }

    fn members(write: &ComputedWrite) -> Vec<(String, f64)> {
        let MochaOperation::Insert { value, expire } = &write.operation else {
            panic!("{:?}", write.operation);
        };
        assert!(matches!(expire, ExpirePolicy::Persistent));
        let ValueObject::ZSet(zset) = &value.data else {
            panic!("not a sorted set");
        };
        zset.lock()
            .zrange(0, -1)
            .into_iter()
            .map(|(member, score)| (String::from_utf8(member.to_vec()).unwrap(), score))
            .collect()
    }

    #[test]
    fn store_writes_the_geohashes_and_storedist_the_distances() {
        let (writes, reply) = stored(store(&["STORE", "dst"]), sicily());
        assert_eq!(reply.encode(), b":2\r\n");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].key.as_ref(), b"dst");
        assert_eq!(
            members(&writes[0]),
            [
                ("Palermo".to_owned(), PALERMO),
                ("Catania".to_owned(), CATANIA)
            ]
        );

        // Distances are stored unrounded, in the unit of the radius.
        let (writes, reply) = stored(store(&["STOREDIST", "dst"]), sicily());
        assert_eq!(reply.encode(), b":2\r\n");
        let distances = members(&writes[0]);
        assert_eq!(distances[0].0, "Catania");
        assert_eq!(distances[1].0, "Palermo");
        assert!((distances[0].1 - 56.4413).abs() < 5e-5, "{distances:?}");
        assert!((distances[1].1 - 190.4424).abs() < 5e-5, "{distances:?}");

        // COUNT and the order choose what is stored.
        let (writes, reply) = stored(store(&["COUNT", "1", "DESC", "STORE", "dst"]), sicily());
        assert_eq!(reply.encode(), b":1\r\n");
        assert_eq!(members(&writes[0]), [("Palermo".to_owned(), PALERMO)]);
    }

    #[test]
    fn store_deletes_the_destination_when_nothing_is_found() {
        let nothing = GeoRadiusCommand
            .raft_request(&items(&[
                "GEORADIUS",
                "Sicily",
                "-120",
                "40",
                "10",
                "km",
                "STORE",
                "dst",
            ]))
            .unwrap();
        let Operation::Redis(RedisOperation::RedisGeoRadiusStore(nothing)) = nothing else {
            panic!("expected a store");
        };
        for (request, source) in [
            (nothing, sicily()),
            (store(&["STORE", "dst"]), None),
            (store(&["STOREDIST", "dst", "COUNT", "1"]), None),
        ] {
            let (writes, reply) = stored(request, source);
            assert_eq!(reply.encode(), b":0\r\n");
            assert_eq!(writes.len(), 1);
            assert_eq!(writes[0].key.as_ref(), b"dst");
            assert!(matches!(writes[0].operation, MochaOperation::Remove));
        }

        // A source of another type is an error and writes nothing.
        let (writes, reply) = stored(store(&["STORE", "dst"]), string());
        assert!(writes.is_empty());
        assert_eq!(reply.encode(), WRONG_TYPE);
    }
}
