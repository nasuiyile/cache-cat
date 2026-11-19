use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::geo::codec::encode_score;
use crate::protocol::geo::parse_long_lat;
use crate::protocol::option_eq;
use crate::protocol::raft_command::RaftCommand;
use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;

/// GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ...]
pub struct GeoAddCommand;

impl GeoAddCommand {
    fn parse(items: &[Value]) -> Result<GeoAddReq, ProtocolError> {
        if items.len() < 5 {
            return Err(ProtocolError::WrongArgCount("geoadd"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let (mut nx, mut xx, mut ch) = (false, false, false);
        let mut index = 2;
        while let Some(option) = items.get(index).and_then(Value::string_bytes_clone) {
            if option_eq(&option, b"NX") {
                nx = true;
            } else if option_eq(&option, b"XX") {
                xx = true;
            } else if option_eq(&option, b"CH") {
                ch = true;
            } else {
                break;
            }
            index += 1;
        }

        // Redis reports incomplete triples and NX with XX as syntax errors
        // before reading any coordinate. ZADD then rejects an empty list.
        let triples = &items[index..];
        if triples.is_empty() || !triples.len().is_multiple_of(3) || (nx && xx) {
            return Err(ProtocolError::SyntaxError);
        }

        // Every pair is validated here, so a bad one adds no member at all.
        let mut members = Vec::with_capacity(triples.len() / 3);
        for triple in triples.chunks_exact(3) {
            let (longitude, latitude) = parse_long_lat(&triple[0], &triple[1])?;
            let member = triple[2]
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("member"))?;
            members.push((member, encode_score(longitude, latitude)));
        }

        Ok(GeoAddReq {
            key,
            nx,
            xx,
            ch,
            members,
        })
    }
}

impl RaftCommand for GeoAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        Ok(Operation::Base(BaseOperation::GeoAdd(Self::parse(items)?)))
    }
}

#[async_trait]
impl Command for GeoAddCommand {
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

        let operation = self.raft_request(items)?;
        server.app.write(operation, client.db_number).await
    }
}

/// Replicated GEOADD. The scores are computed once while parsing, so applying
/// the log entry is a sorted-set update without floating-point arithmetic.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GeoAddReq {
    pub key: Bytes,
    pub nx: bool,
    pub xx: bool,
    pub ch: bool,
    /// Members with their 52-bit geohash scores.
    pub members: Vec<(Bytes, f64)>,
}

impl fmt::Display for GeoAddReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GeoAddReq {{ key: {}, nx: {}, xx: {}, ch: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.nx,
            self.xx,
            self.ch,
            self.members
        )
    }
}

impl From<GeoAddReq> for ZAddReq {
    fn from(request: GeoAddReq) -> Self {
        ZAddReq {
            key: request.key,
            nx: request.nx,
            xx: request.xx,
            gt: false,
            lt: false,
            ch: request.ch,
            members: request.members,
        }
    }
}

/// Redis executes GEOADD by calling ZADD with the computed scores, so both
/// paths share the sorted-set update and its reply.
impl ComputeCommand for GeoAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::GeoAdd(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        ZAddReq::from(self).mutate(entry, write_clock)
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        ZAddReq::from(self).init()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<Value> {
        values
            .iter()
            .map(|value| Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes()))))
            .collect()
    }

    fn error(values: &[&str]) -> String {
        GeoAddCommand::parse(&args(values)).unwrap_err().to_string()
    }

    #[test]
    fn options_use_c_string_comparison_and_stop_at_the_first_coordinate() {
        let request = GeoAddCommand::parse(&args(&[
            "GEOADD",
            "key",
            "ch",
            "Nx\0ignored",
            "CH",
            "13.361389",
            "38.115556",
            "XX",
        ]))
        .unwrap();
        assert!(request.nx && request.ch && !request.xx);
        // A member may be named like an option.
        assert_eq!(
            request.members,
            vec![(Bytes::from_static(b"XX"), 3479099956230698.0)]
        );
    }

    #[test]
    fn arity_and_syntax_errors_precede_coordinate_validation() {
        for values in [vec!["GEOADD"], vec!["GEOADD", "key", "1", "2"]] {
            assert_eq!(
                error(&values),
                "ERR wrong number of arguments for 'geoadd' command"
            );
        }
        for values in [
            vec!["GEOADD", "key", "1", "2", "member", "3"],
            vec!["GEOADD", "key", "bad", "bad", "member", "3", "4"],
            vec!["GEOADD", "key", "NX", "XX", "1", "2", "member"],
            vec!["GEOADD", "key", "NX", "XX", "bad", "bad", "member"],
            // ZADD's GT and LT are not GEOADD options.
            vec!["GEOADD", "key", "GT", "1", "2", "member"],
            vec!["GEOADD", "key", "NX", "CH", "CH"],
        ] {
            assert_eq!(error(&values), "ERR syntax error", "{values:?}");
        }
    }

    #[test]
    fn coordinates_are_parsed_and_range_checked_like_redis() {
        for (longitude, latitude) in [
            ("abc", "1"),
            ("1", "abc"),
            ("nan", "1"),
            ("1", ""),
            (" 1", "1"),
            ("1e400", "1"),
            // Both numbers are parsed before the range check.
            ("200", "abc"),
        ] {
            assert_eq!(
                error(&["GEOADD", "key", longitude, latitude, "member"]),
                "ERR value is not a valid float",
                "{longitude:?},{latitude:?}"
            );
        }
        for (longitude, latitude, message) in [
            ("180.000001", "0", "180.000001,0.000000"),
            ("-181", "10.5", "-181.000000,10.500000"),
            ("0", "85.05112879", "0.000000,85.051129"),
            ("0", "-90", "0.000000,-90.000000"),
            ("inf", "0", "inf,0.000000"),
            ("0", "-inf", "0.000000,-inf"),
        ] {
            assert_eq!(
                error(&["GEOADD", "key", longitude, latitude, "member"]),
                format!("ERR invalid longitude,latitude pair {message}")
            );
        }
        // One invalid pair rejects the command, including the valid members.
        assert_eq!(
            error(&["GEOADD", "key", "1", "2", "valid", "1", "86", "invalid"]),
            "ERR invalid longitude,latitude pair 1.000000,86.000000"
        );
        for (longitude, latitude) in [
            ("-180", "-85.05112878"),
            ("180", "85.05112878"),
            ("0x10", "+.5e1"),
        ] {
            assert!(
                GeoAddCommand::parse(&args(&["GEOADD", "key", longitude, latitude, "member"]))
                    .is_ok()
            );
        }
    }

    #[test]
    fn the_log_entry_is_a_geoadd_and_applies_as_a_sorted_set_update() {
        let operation = GeoAddCommand
            .raft_request(&args(&["GEOADD", "key", "XX", "CH", "1", "2", "member"]))
            .unwrap();
        let Operation::Base(BaseOperation::GeoAdd(request)) = operation else {
            panic!("expected GEOADD");
        };
        assert!(matches!(
            request.clone().into_base_op(),
            BaseOperation::GeoAdd(_)
        ));
        // XX never creates the key.
        let (operation, response) = request.init();
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(response.encode(), b":0\r\n");
    }
}
