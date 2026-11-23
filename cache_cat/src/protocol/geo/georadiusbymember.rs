use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::codec::decode_score;
use crate::protocol::geo::distance::GeoUnit;
use crate::protocol::geo::radius::{RadiusOptions, parse_radius, store};
use crate::protocol::geo::search::GeoPoint;
use crate::protocol::geo::sorted_set;
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

/// GEORADIUSBYMEMBER key member radius <M | KM | FT | MI> [WITHCOORD]
/// [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC | DESC]
/// [STORE key | STOREDIST key]
///
/// GEORADIUS around the position of a member of the index. Like GEORADIUS it
/// takes the read path unless STORE or STOREDIST makes it a write.
pub struct GeoRadiusByMemberCommand;

/// The radius with its unit, or the error reply for them.
///
/// Redis reads these two arguments only when the key exists and holds the
/// member, so the error is decided while parsing but sent only then.
type Radius = Result<(f64, GeoUnit), String>;

/// GEORADIUSBYMEMBER that replies with the members it finds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadiusByMemberParams {
    pub key: Bytes,
    pub member: Bytes,
    pub radius: Radius,
    /// `Err` holds the error reply for the options, which Redis reads after
    /// the key, the member and the radius.
    pub options: Result<RadiusOptions, String>,
}

/// GEORADIUSBYMEMBER with STORE or STOREDIST, which writes the members it
/// finds to another key as a sorted set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadiusByMemberStoreReq {
    pub key: Bytes,
    pub member: Bytes,
    pub radius: Radius,
    pub options: RadiusOptions,
    pub destination: Bytes,
    /// STOREDIST: the score of a member is its distance in the unit of the
    /// radius instead of its geohash.
    pub store_dist: bool,
}

/// The members around `member` with the unit of their distances, or `None`
/// without a key, or the error reply.
///
/// Redis `georadiusGeneric` checks in this order: the type of the key, then
/// with a key the member and the radius, then the options. Without a key it
/// reads neither the member nor the radius.
fn members_around(
    source: Option<EntrySnapshot<MyValue>>,
    member: &Bytes,
    radius: &Radius,
    options: Result<&RadiusOptions, &String>,
) -> Result<Option<(Vec<GeoPoint>, GeoUnit)>, Value> {
    let rejected = |message: &String| Value::error(message.clone());
    let Some(zset) = sorted_set(source).map_err(Value::from)? else {
        options.map_err(rejected)?;
        return Ok(None);
    };

    // One lock covers the lookup of the centre and the scan around it, so
    // both see the same index.
    let index = zset.lock();
    let score = index
        .zscore(member)
        .ok_or_else(|| Value::error("ERR could not decode requested zset member"))?;
    let (radius, unit) = *radius.as_ref().map_err(rejected)?;
    let options = options.map_err(rejected)?;
    // The centre of the member's cell, the position GEOPOS reports.
    let (longitude, latitude) = decode_score(score);
    let search = options.search(longitude, latitude, radius, unit);
    let scan = search.scan(&index);
    drop(index);
    Ok(Some((search.finish(scan), unit)))
}

impl ReadCommand for GeoRadiusByMemberParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let found = members_around(value, &self.member, &self.radius, self.options.as_ref());
        match (found, &self.options) {
            (Err(reply), _) => reply,
            (Ok(Some((points, unit))), Ok(options)) => options.reply(points, unit),
            // Without a key there is nothing to find.
            (Ok(_), _) => Value::Array(Some(Vec::new())),
        }
    }
}

impl MultiReadComputeCommand for GeoRadiusByMemberStoreReq {
    fn read_keys(&self) -> impl Iterator<Item = &Bytes> {
        std::iter::once(&self.key)
    }

    fn mutate_writes(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (Vec<ComputedWrite>, Value) {
        let source = read_entries.into_iter().next().flatten();
        match members_around(source, &self.member, &self.radius, Ok(&self.options)) {
            Ok(found) => {
                // Without a key nothing is stored, whatever the radius says.
                let (points, unit) = found.unwrap_or((Vec::new(), GeoUnit::Meters));
                store(self.destination, self.store_dist, unit, points)
            }
            Err(reply) => (Vec::new(), reply),
        }
    }
}

/// Writes the key, the member and the radius as the client sent them.
fn write_centre(
    f: &mut fmt::Formatter<'_>,
    key: &Bytes,
    member: &Bytes,
    radius: &Radius,
) -> fmt::Result {
    write!(
        f,
        "GEORADIUSBYMEMBER {} {}",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(member)
    )?;
    match radius {
        Ok((radius, unit)) => write!(f, " {radius} {unit}"),
        Err(message) => write!(f, " (rejected: {message})"),
    }
}

impl Display for GeoRadiusByMemberParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_centre(f, &self.key, &self.member, &self.radius)?;
        match &self.options {
            Ok(options) => write!(f, "{options}"),
            Err(message) => write!(f, " (rejected: {message})"),
        }
    }
}

impl Display for GeoRadiusByMemberStoreReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_centre(f, &self.key, &self.member, &self.radius)?;
        write!(
            f,
            "{} {} {}",
            self.options,
            if self.store_dist {
                "STOREDIST"
            } else {
                "STORE"
            },
            String::from_utf8_lossy(&self.destination)
        )
    }
}

impl RaftCommand for GeoRadiusByMemberCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        // Only the arity is checked before the key is looked up.
        if items.len() < 5 {
            return Err(ProtocolError::WrongArgCount("georadiusbymember"));
        }
        let bytes = |item: &Value, name| {
            item.string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument(name))
        };
        let key = bytes(&items[1], "key")?;
        let member = bytes(&items[2], "member")?;
        let radius = parse_radius(&items[3], &items[4]).map_err(|error| error.to_string());
        let options = match RadiusOptions::parse(&items[5..]) {
            Ok((options, Some((destination, store_dist)))) => {
                return Ok(Operation::Redis(
                    RedisOperation::RedisGeoRadiusByMemberStore(Box::new(
                        GeoRadiusByMemberStoreReq {
                            key,
                            member,
                            radius,
                            options,
                            destination,
                            store_dist,
                        },
                    )),
                ));
            }
            Ok((options, None)) => Ok(options),
            // Nothing is written, so the read path reports the error.
            Err(error) => Err(error.to_string()),
        };
        Ok(Operation::Read(ReadOperation::GeoRadiusByMember(
            GeoRadiusByMemberParams {
                key,
                member,
                radius,
                options,
            },
        )))
    }
}

#[async_trait]
impl Command for GeoRadiusByMemberCommand {
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
    use crate::protocol::geo::codec::encode_score;
    use crate::protocol::geo::distance::{distance, distance_reply};
    use crate::protocol::geo::search::GeoOrder;
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use crate::raft::types::core::value_object::ValueObject;
    use parking_lot::Mutex;
    use std::sync::Arc;

    // The three cities of the Redis documentation example.
    const CITIES: [(&str, f64, f64); 3] = [
        ("Agrigento", 13.583333, 37.316667),
        ("Palermo", 13.361389, 38.115556),
        ("Catania", 15.087269, 37.502669),
    ];
    const WRONG_TYPE: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    const NO_MEMBER: &[u8] = b"-ERR could not decode requested zset member\r\n";

    fn items(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    /// The request for `GEORADIUSBYMEMBER Sicily` followed by `arguments`.
    fn request(arguments: &[&str]) -> Operation {
        let mut values = vec!["GEORADIUSBYMEMBER", "Sicily"];
        values.extend(arguments);
        GeoRadiusByMemberCommand
            .raft_request(&items(&values))
            .unwrap()
    }

    fn params(arguments: &[&str]) -> GeoRadiusByMemberParams {
        match request(arguments) {
            Operation::Read(ReadOperation::GeoRadiusByMember(params)) => params,
            other => panic!("{arguments:?} is not a read: {other:?}"),
        }
    }

    fn store_request(arguments: &[&str]) -> GeoRadiusByMemberStoreReq {
        match request(arguments) {
            Operation::Redis(RedisOperation::RedisGeoRadiusByMemberStore(request)) => *request,
            other => panic!("{arguments:?} is not a store: {other:?}"),
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
        for (name, longitude, latitude) in CITIES {
            zset.zincrby(name.into(), encode_score(longitude, latitude));
        }
        entry(ValueObject::ZSet(Arc::new(Mutex::new(zset))))
    }

    fn string() -> Option<EntrySnapshot<MyValue>> {
        entry(ValueObject::String(Bytes::from_static(b"value")))
    }

    fn reply(arguments: &[&str], proto: u8) -> String {
        String::from_utf8(params(arguments).execute(sicily()).encode_proto(proto)).unwrap()
    }

    /// What GEOPOS reports for a city, and the RESP2 pair of that position.
    fn position(city: usize) -> ((f64, f64), String) {
        let (_, longitude, latitude) = CITIES[city];
        let (longitude, latitude) = decode_score(encode_score(longitude, latitude));
        let pair = Value::Array(Some(vec![
            Value::Double(longitude),
            Value::Double(latitude),
        ]));
        (
            (longitude, latitude),
            String::from_utf8(pair.encode()).unwrap(),
        )
    }

    /// What GEODIST reports for two cities in kilometers, as a bulk string.
    fn kilometers(from: usize, to: usize) -> String {
        let ((longitude1, latitude1), _) = position(from);
        let ((longitude2, latitude2), _) = position(to);
        let meters = distance(longitude1, latitude1, longitude2, latitude2);
        String::from_utf8(distance_reply(meters / 1000.0).encode()).unwrap()
    }

    #[test]
    fn replies_with_the_members_of_the_redis_documentation_example() {
        // Agrigento itself and Palermo, as the documentation shows.
        let members = "*2\r\n$9\r\nAgrigento\r\n$7\r\nPalermo\r\n";
        assert_eq!(reply(&["Agrigento", "100", "km"], 2), members);
        assert_eq!(reply(&["Agrigento", "100", "km"], 3), members);
        assert_eq!(reply(&["Agrigento", "100000", "M"], 2), members);

        // The distances are those of GEODIST: the member is at distance zero
        // from the centre, which is its own position.
        let palermo = kilometers(0, 1);
        assert!(palermo.starts_with("$7\r\n90.97"), "{palermo}");
        assert_eq!(
            reply(&["Agrigento", "100", "km", "WITHDIST"], 2),
            format!(
                "*2\r\n*2\r\n$9\r\nAgrigento\r\n$6\r\n0.0000\r\n*2\r\n$7\r\nPalermo\r\n{palermo}"
            )
        );
        // The coordinates are those of GEOPOS.
        assert_eq!(
            reply(&["Agrigento", "100", "km", "WITHCOORD"], 2),
            format!(
                "*2\r\n*2\r\n$9\r\nAgrigento\r\n{}*2\r\n$7\r\nPalermo\r\n{}",
                position(0).1,
                position(1).1
            )
        );
        assert!(reply(&["Agrigento", "100", "km", "WITHCOORD"], 3).contains("\r\n*2\r\n,13.58"));

        // A larger radius reaches Catania, and the centre may be any member.
        assert_eq!(
            reply(&["Agrigento", "200", "km", "ASC"], 2),
            "*3\r\n$9\r\nAgrigento\r\n$7\r\nPalermo\r\n$7\r\nCatania\r\n"
        );
        assert_eq!(
            reply(&["Catania", "200", "km", "WITHDIST", "DESC"], 2),
            format!(
                "*3\r\n*2\r\n$7\r\nPalermo\r\n{}*2\r\n$9\r\nAgrigento\r\n{}*2\r\n$7\r\nCatania\r\n$6\r\n0.0000\r\n",
                kilometers(2, 1),
                kilometers(2, 0)
            )
        );
        // COUNT alone keeps the nearest members: the centre comes first.
        assert_eq!(
            reply(&["Palermo", "500", "km", "COUNT", "2"], 2),
            "*2\r\n$7\r\nPalermo\r\n$9\r\nAgrigento\r\n"
        );
        assert_eq!(
            reply(&["Palermo", "0", "m", "WITHHASH"], 2),
            "*1\r\n*2\r\n$7\r\nPalermo\r\n:3479099956230698\r\n"
        );
    }

    #[test]
    fn only_the_arity_is_checked_before_the_key() {
        for values in [
            &["GEORADIUSBYMEMBER"][..],
            &["GEORADIUSBYMEMBER", "key"],
            &["GEORADIUSBYMEMBER", "key", "member", "100"],
        ] {
            assert!(matches!(
                GeoRadiusByMemberCommand.raft_request(&items(values)),
                Err(ProtocolError::WrongArgCount("georadiusbymember"))
            ));
        }
    }

    #[test]
    fn without_a_key_only_the_options_are_read() {
        // Neither the member nor the radius nor the unit is looked at.
        for arguments in [
            &["Agrigento", "100", "km"][..],
            &["nobody", "far", "parsecs"],
            &[
                "nobody", "-1", "km", "WITHDIST", "COUNT", "3", "ANY", "DESC",
            ],
        ] {
            let params = params(arguments);
            for proto in [2, 3] {
                assert_eq!(
                    params.execute(None).encode_proto(proto),
                    b"*0\r\n",
                    "{arguments:?}"
                );
            }
        }
        for (arguments, error) in [
            (
                &["nobody", "far", "parsecs", "BOGUS"][..],
                "ERR syntax error",
            ),
            (&["nobody", "far", "parsecs", "COUNT"], "ERR syntax error"),
            (
                &["nobody", "far", "parsecs", "COUNT", "0"],
                "ERR COUNT must be > 0",
            ),
            (
                &["nobody", "far", "parsecs", "ANY"],
                "ERR the ANY argument requires COUNT argument",
            ),
            (
                &["nobody", "far", "parsecs", "STORE", "dst", "WITHHASH"],
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ),
        ] {
            let params = params(arguments);
            assert_eq!(
                String::from_utf8(params.execute(None).encode()).unwrap(),
                format!("-{error}\r\n"),
                "{arguments:?}"
            );
            // A key of another type is reported before any of it.
            assert_eq!(params.execute(string()).encode(), WRONG_TYPE);
        }
    }

    #[test]
    fn with_a_key_the_member_the_radius_and_the_options_are_read_in_that_order() {
        for (arguments, error) in [
            (
                &["nobody", "100", "km"][..],
                "ERR could not decode requested zset member",
            ),
            (
                &["nobody", "far", "parsecs", "BOGUS"],
                "ERR could not decode requested zset member",
            ),
            (
                &["Agrigento", "far", "parsecs", "BOGUS"],
                "ERR need numeric radius",
            ),
            (&["Agrigento", "nan", "km"], "ERR need numeric radius"),
            (
                &["Agrigento", "-1", "parsecs", "BOGUS"],
                "ERR radius cannot be negative",
            ),
            (
                &["Agrigento", "100", "parsecs", "BOGUS"],
                "ERR unsupported unit provided. please use M, KM, FT, MI",
            ),
            (&["Agrigento", "100", "km", "BOGUS"], "ERR syntax error"),
            (&["Agrigento", "100", "km", "STORE"], "ERR syntax error"),
            (
                &["Agrigento", "100", "km", "COUNT", "x"],
                "ERR value is not an integer or out of range",
            ),
            (
                &["Agrigento", "100", "km", "COUNT", "2", "ANY", "COUNT", "0"],
                "ERR COUNT must be > 0",
            ),
            (
                &["Agrigento", "100", "km", "ANY"],
                "ERR the ANY argument requires COUNT argument",
            ),
            (
                &["Agrigento", "100", "km", "WITHCOORD", "STOREDIST", "dst"],
                "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ),
        ] {
            let params = params(arguments);
            assert_eq!(
                String::from_utf8(params.execute(sicily()).encode()).unwrap(),
                format!("-{error}\r\n"),
                "{arguments:?}"
            );
            assert_eq!(params.execute(string()).encode(), WRONG_TYPE);
        }
        assert_eq!(
            params(&["Agrigento", "100", "km"])
                .execute(string())
                .encode(),
            WRONG_TYPE
        );
    }

    #[test]
    fn requests_keep_what_the_client_sent() {
        let parsed = params(&[
            "Agrigento",
            "100",
            "KM",
            "withdist",
            "Count",
            "7",
            "any",
            "desc",
        ]);
        assert_eq!(parsed.member.as_ref(), b"Agrigento");
        assert_eq!(parsed.radius, Ok((100.0, GeoUnit::Kilometers)));
        let options = parsed.options.as_ref().unwrap();
        assert!(options.with_dist && !options.with_hash && !options.with_coord);
        assert_eq!(
            (options.count, options.any, options.order),
            (Some(7), true, Some(GeoOrder::Desc))
        );
        assert_eq!(
            parsed.to_string(),
            "GEORADIUSBYMEMBER Sicily Agrigento 100 km COUNT 7 ANY DESC WITHDIST"
        );
        assert_eq!(
            params(&["Agrigento", "far", "km", "BOGUS"]).to_string(),
            "GEORADIUSBYMEMBER Sicily Agrigento (rejected: ERR need numeric radius) \
             (rejected: ERR syntax error)"
        );

        // A destination may be named like an option, and the last STORE or
        // STOREDIST decides both the key and the scores.
        let request = store_request(&["Agrigento", "1", "mi", "STORE", "ANY", "storedist", "ASC"]);
        assert_eq!(request.destination.as_ref(), b"ASC");
        assert!(request.store_dist);
        assert_eq!(request.options.order, None);
        assert_eq!(
            request.to_string(),
            "GEORADIUSBYMEMBER Sicily Agrigento 1 mi STOREDIST ASC"
        );
        // A radius that cannot be read still makes a store request: without
        // a key Redis deletes the destination before it would read it.
        let request = store_request(&["Agrigento", "far", "km", "COUNT", "2", "STORE", "dst"]);
        assert_eq!(request.radius, Err("ERR need numeric radius".to_owned()));
        assert_eq!(
            request.to_string(),
            "GEORADIUSBYMEMBER Sicily Agrigento (rejected: ERR need numeric radius) COUNT 2 STORE dst"
        );
    }

    fn stored(
        request: GeoRadiusByMemberStoreReq,
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
        let score = |city: usize| encode_score(CITIES[city].1, CITIES[city].2);
        let (writes, reply) = stored(
            store_request(&["Agrigento", "100", "km", "STORE", "dst"]),
            sicily(),
        );
        assert_eq!(reply.encode(), b":2\r\n");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].key.as_ref(), b"dst");
        assert_eq!(
            members(&writes[0]),
            [
                ("Agrigento".to_owned(), score(0)),
                ("Palermo".to_owned(), score(1))
            ]
        );

        // Distances are stored unrounded, in the unit of the radius.
        let (writes, reply) = stored(
            store_request(&["Agrigento", "100", "km", "STOREDIST", "dst"]),
            sicily(),
        );
        assert_eq!(reply.encode(), b":2\r\n");
        let distances = members(&writes[0]);
        assert_eq!(distances[0], ("Agrigento".to_owned(), 0.0));
        assert_eq!(distances[1].0, "Palermo");
        assert!((90.97..90.98).contains(&distances[1].1), "{distances:?}");

        // COUNT and the order choose what is stored.
        let (writes, reply) = stored(
            store_request(&[
                "Agrigento",
                "200",
                "km",
                "COUNT",
                "1",
                "DESC",
                "STORE",
                "dst",
            ]),
            sicily(),
        );
        assert_eq!(reply.encode(), b":1\r\n");
        assert_eq!(members(&writes[0]), [("Catania".to_owned(), score(2))]);
    }

    #[test]
    fn store_deletes_the_destination_without_a_key_and_writes_nothing_on_an_error() {
        // Without a key the destination is deleted, whatever the member and
        // the radius say.
        for arguments in [
            &["Agrigento", "100", "km", "STORE", "dst"][..],
            &["nobody", "far", "parsecs", "STOREDIST", "dst", "COUNT", "1"],
        ] {
            let (writes, reply) = stored(store_request(arguments), None);
            assert_eq!(reply.encode(), b":0\r\n", "{arguments:?}");
            assert_eq!(writes.len(), 1);
            assert_eq!(writes[0].key.as_ref(), b"dst");
            assert!(matches!(writes[0].operation, MochaOperation::Remove));
        }

        // With a key, a missing member or a bad radius is an error.
        for (arguments, error) in [
            (&["nobody", "100", "km", "STORE", "dst"][..], NO_MEMBER),
            (&["nobody", "far", "parsecs", "STORE", "dst"], NO_MEMBER),
            (
                &["Agrigento", "far", "km", "STORE", "dst"],
                b"-ERR need numeric radius\r\n",
            ),
            (
                &["Agrigento", "100", "parsecs", "STOREDIST", "dst"],
                b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n",
            ),
        ] {
            let (writes, reply) = stored(store_request(arguments), sicily());
            assert!(writes.is_empty(), "{arguments:?}");
            assert_eq!(reply.encode(), error, "{arguments:?}");
        }
        let (writes, reply) = stored(
            store_request(&["Agrigento", "100", "km", "STORE", "dst"]),
            string(),
        );
        assert!(writes.is_empty());
        assert_eq!(reply.encode(), WRONG_TYPE);
    }
}
