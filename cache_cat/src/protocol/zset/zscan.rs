//! ZSCAN key cursor [MATCH pattern] [COUNT count].

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::common::scan::parse_cursor;
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::{Value, format_double};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    // Redis validates the cursor before lookup, but options after checking
    // that the key exists and has the expected type, including in EXEC/Lua.
    option_error: Option<String>,
}

impl Display for ZScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZSCAN {} {} COUNT {}",
            String::from_utf8_lossy(&self.key),
            self.cursor,
            self.count
        )?;
        if let Some(pattern) = &self.pattern {
            write!(f, " MATCH {}", String::from_utf8_lossy(pattern))?;
        }
        Ok(())
    }
}

impl ReadCommand for ZScanParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        self.execute_scan::<false>(value)
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, _read_clock: u64) -> Value {
        self.execute_scan::<true>(value)
    }
}

impl ZScanParams {
    fn execute_scan<const DETERMINISTIC: bool>(
        &self,
        value: Option<EntrySnapshot<MyValue>>,
    ) -> Value {
        let Some(snapshot) = value else {
            return Self::reply(0, Vec::new());
        };
        let ValueObject::ZSet(zset) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        if let Some(error) = &self.option_error {
            return Value::error(error.clone());
        }
        let guard = zset.lock();
        let matcher = self.pattern.as_deref().map(GlobMatcher::new);
        let matches = |member: &Bytes| {
            matcher
                .as_ref()
                .is_none_or(|pattern| pattern.matches(member))
        };
        let mut result = Vec::with_capacity(self.count.min(guard.len()).min(256) * 2);

        if DETERMINISTIC {
            // Hash seeds, bucket capacities and scan cursors are node-local.
            // Replicated reads return all matches in member order and finish
            // the scan, accepting a cursor from an earlier ordinary read.
            let mut entries: Vec<_> = guard.iter().filter(|(member, _)| matches(member)).collect();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (member, score) in entries {
                Self::push_entry(&mut result, member, *score);
            }
            return Self::reply(0, result);
        }

        let cursor = guard.scan(self.cursor, self.count, |member, score| {
            if matches(member) {
                Self::push_entry(&mut result, member, *score);
            }
        });
        Self::reply(cursor, result)
    }

    fn push_entry(result: &mut Vec<Value>, member: &Bytes, score: f64) {
        result.push(Value::BulkString(Some(member.clone())));
        // Unlike ZSCORE, ZSCAN returns score strings even with RESP3.
        result.push(Value::BulkString(Some(format_scan_score(score))));
    }

    fn reply(cursor: u64, entries: Vec<Value>) -> Value {
        Value::Array(Some(vec![
            Value::BulkString(Some(cursor.to_string().into())),
            Value::Array(Some(entries)),
        ]))
    }
}

/// Redis's hash-table ZSCAN uses ld2string(LD_STR_AUTO), i.e. %.17Lg,
/// after promoting the stored double without changing its value. Round to
/// seventeen significant digits and use C's %g exponent/zero trimming rules.
fn format_scan_score(score: f64) -> Bytes {
    if !score.is_finite() || score == 0.0 {
        return format_double(score).into();
    }
    if score == score.trunc() && score.abs() < 1e17 {
        return (score as i64).to_string().into();
    }
    let scientific = format!("{score:.16e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
    if !(-4..17).contains(&exponent) {
        return format!("{mantissa}e{exponent:+03}").into();
    }
    let negative = mantissa.starts_with('-');
    let digits = mantissa.trim_start_matches('-').replace('.', "");
    let decimal_position = exponent + 1;
    let mut result = String::with_capacity(24);
    if negative {
        result.push('-');
    }
    if decimal_position <= 0 {
        result.push_str("0.");
        for _ in 0..-decimal_position {
            result.push('0');
        }
        result.push_str(&digits);
    } else {
        let decimal_position = decimal_position as usize;
        if decimal_position < digits.len() {
            result.push_str(&digits[..decimal_position]);
            result.push('.');
            result.push_str(&digits[decimal_position..]);
        } else {
            result.push_str(&digits);
            for _ in digits.len()..decimal_position {
                result.push('0');
            }
        }
    }
    result.into()
}

pub struct ZScanCommand;

impl ZScanCommand {
    fn parse_args(items: &[Value]) -> Result<ZScanParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("zscan"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let cursor = items[2]
            .string_bytes_clone()
            .and_then(|value| parse_cursor(&value))
            .ok_or(ProtocolError::InvalidArgument("cursor"))?;
        let mut params = ZScanParams {
            key,
            cursor,
            count: 10,
            pattern: None,
            option_error: None,
        };
        if let Err(error) = Self::parse_options(&mut params, &items[3..]) {
            params.option_error = Some(error.to_string());
        }
        Ok(params)
    }

    fn parse_options(params: &mut ZScanParams, items: &[Value]) -> Result<(), ProtocolError> {
        let mut index = 0;
        while index < items.len() {
            let option = items[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;
            if option_eq(&option, b"COUNT") && index + 1 < items.len() {
                let count = items[index + 1].try_parse_canonical_i64()?;
                if count < 1 {
                    return Err(ProtocolError::SyntaxError);
                }
                params.count = usize::try_from(count).map_err(|_| ProtocolError::NotAnInteger)?;
                index += 2;
            } else if option_eq(&option, b"MATCH") && index + 1 < items.len() {
                let pattern = items[index + 1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::SyntaxError)?;
                params.pattern = (pattern.as_ref() != b"*").then_some(pattern);
                index += 2;
            } else if option_eq(&option, b"NOVALUES") {
                return Err(ProtocolError::response(
                    "ERR NOVALUES option can only be used in HSCAN",
                ));
            } else {
                return Err(ProtocolError::SyntaxError);
            }
        }
        Ok(())
    }
}

impl ReadRaftCommand for ZScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::ZScan(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for ZScanCommand {
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
        server
            .app
            .read(self.read_operation(items)?, client.db_number)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::zset::zadd::ZAddReq;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::{do_request, read_request};
    use crate::raft::types::core::structure::sorted_set::SortedSet;
    use parking_lot::Mutex;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;

    fn arguments(args: &[&[u8]]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg))))
            .collect()
    }

    fn parse(args: &[&[u8]]) -> Result<ZScanParams, ProtocolError> {
        ZScanCommand::parse_args(&arguments(args))
    }

    fn add(members: Vec<(Bytes, f64)>) -> ZAddReq {
        ZAddReq {
            key: "z".into(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
            members,
        }
    }

    fn sorted_set(members: impl IntoIterator<Item = (Bytes, f64)>) -> SortedSet {
        let mut result = SortedSet::new();
        result.zadd(add(members.into_iter().collect()));
        result
    }

    fn snapshot(set: SortedSet) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(set)))),
            expire_at: None,
        }
    }

    fn page(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut outer)) = reply else {
            panic!("expected scan array, got {reply:?}");
        };
        assert_eq!(outer.len(), 2);
        let Value::Array(Some(values)) = outer.pop().unwrap() else {
            panic!("expected flat member/score array");
        };
        let cursor = outer.pop().unwrap().string_bytes_clone().unwrap();
        let cursor = std::str::from_utf8(&cursor).unwrap().parse().unwrap();
        assert_eq!(values.len() % 2, 0);
        (
            cursor,
            values
                .into_iter()
                .map(|value| {
                    let Value::BulkString(Some(value)) = value else {
                        panic!("ZSCAN members and scores must be bulk strings");
                    };
                    value
                })
                .collect(),
        )
    }

    #[test]
    fn cursor_and_options_preserve_redis_validation_order() {
        for (input, expected) in [
            (b"0".as_slice(), 0),
            (b"01", 1),
            (b"+1", 1),
            (b" 1", 1),
            (b"1\0junk", 1),
            (b"-0", 0),
            (b"-01", u64::MAX),
            (b"18446744073709551615", u64::MAX),
        ] {
            assert_eq!(parse(&[b"ZSCAN", b"z", input]).unwrap().cursor, expected);
        }
        for input in [
            b"".as_slice(),
            b"-1",
            b"1 ",
            b"1.0",
            b"18446744073709551616",
            b"\xff",
        ] {
            assert_eq!(
                parse(&[b"ZSCAN", b"z", input]).unwrap_err(),
                ProtocolError::InvalidArgument("cursor")
            );
        }
        assert_eq!(
            parse(&[b"ZSCAN", b"z"]).unwrap_err(),
            ProtocolError::WrongArgCount("zscan")
        );
        let params = parse(&[
            b"ZSCAN",
            b"z",
            b"0",
            b"COUNT\0ignored",
            b"1",
            b"match",
            b"a*",
            b"count",
            b"2",
            b"MATCH",
            b"*",
        ])
        .unwrap();
        assert_eq!(params.count, 2);
        assert!(params.pattern.is_none());
        assert!(params.option_error.is_none());
        for (options, error) in [
            (vec![b"COUNT".as_slice(), b"0"], ProtocolError::SyntaxError),
            (vec![b"COUNT".as_slice(), b"-1"], ProtocolError::SyntaxError),
            (
                vec![b"COUNT".as_slice(), b"01"],
                ProtocolError::NotAnInteger,
            ),
            (
                vec![b"COUNT".as_slice(), b"9223372036854775808"],
                ProtocolError::NotAnInteger,
            ),
            (vec![b"COUNT".as_slice()], ProtocolError::SyntaxError),
            (vec![b"MATCH".as_slice()], ProtocolError::SyntaxError),
            (
                vec![b"TYPE".as_slice(), b"zset"],
                ProtocolError::SyntaxError,
            ),
            (
                vec![b"NOVALUES".as_slice()],
                ProtocolError::response("ERR NOVALUES option can only be used in HSCAN"),
            ),
        ] {
            let mut args = vec![b"ZSCAN".as_slice(), b"z", b"0"];
            args.extend(options);
            let params = parse(&args).unwrap();
            assert_eq!(page(params.execute(None)), (0, Vec::new()));
            let wrong_type = EntrySnapshot {
                value: MyValue::new(ValueObject::Int(1)),
                expire_at: None,
            };
            assert_eq!(
                params.execute(Some(wrong_type)).encode(),
                Value::from(ProtocolError::WrongType).encode()
            );
            assert_eq!(
                params.execute(Some(snapshot(SortedSet::new()))).encode(),
                Value::from(error).encode()
            );
            let restored: ZScanParams =
                bincode2::deserialize(&bincode2::serialize(&params).unwrap()).unwrap();
            assert_eq!(restored.option_error, params.option_error);
        }
    }

    #[test]
    fn score_strings_match_redis_seventeen_significant_digits() {
        for (score, expected) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.5, "1.5"),
            (42.0, "42"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (0.1, "0.10000000000000001"),
            (0.3, "0.29999999999999999"),
            (1e-4, "0.0001"),
            (1e-5, "1.0000000000000001e-05"),
            (1e16, "10000000000000000"),
            (1e17, "1e+17"),
            (-1e20, "-1e+20"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::from_bits(1), "4.9406564584124654e-324"),
        ] {
            assert_eq!(
                format_scan_score(score).as_ref(),
                expected.as_bytes(),
                "{score}"
            );
        }
        let entry = snapshot(sorted_set([(Bytes::from_static(b"a"), 0.1)]));
        let params = parse(&[b"ZSCAN", b"z", b"0"]).unwrap();
        let reply = params.execute(Some(entry));
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto).as_slice(),
                b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$19\r\n0.10000000000000001\r\n"
            );
        }
    }

    #[test]
    fn full_scan_covers_binary_members_with_flat_replies() {
        let mut expected: BTreeMap<_, _> = (0..257)
            .map(|i| {
                (
                    Bytes::from(format!("member:{i}")),
                    Bytes::from(i.to_string()),
                )
            })
            .collect();
        let mut set = sorted_set((0..257).map(|i| (Bytes::from(format!("member:{i}")), i as f64)));
        set.zadd(add(vec![
            (Bytes::new(), f64::NEG_INFINITY),
            (Bytes::from_static(b"\0\xff"), f64::INFINITY),
        ]));
        expected.insert(Bytes::new(), Bytes::from_static(b"-inf"));
        expected.insert(Bytes::from_static(b"\0\xff"), Bytes::from_static(b"inf"));
        let entry = snapshot(set);
        let mut params = parse(&[b"ZSCAN", b"z", b"0", b"COUNT", b"3"]).unwrap();
        let mut actual = BTreeMap::new();
        let mut calls = 0;
        loop {
            let reply = params.execute(Some(entry.clone()));
            assert_eq!(reply.encode_proto(2), reply.encode_proto(3));
            let (cursor, values) = page(reply);
            for pair in values.chunks_exact(2) {
                assert!(actual.insert(pair[0].clone(), pair[1].clone()).is_none());
            }
            calls += 1;
            assert!(calls <= 1024);
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert!(calls > 1);
        assert_eq!(actual, expected);
    }

    #[test]
    fn match_filters_members_after_count_without_scanning_to_fill_a_page() {
        let set = sorted_set((0..256).map(|i| (Bytes::from(format!("member:{i}")), i as f64)));
        let entry = snapshot(set);
        let params =
            parse(&[b"ZSCAN", b"z", b"0", b"MATCH", b"missing:*", b"COUNT", b"1"]).unwrap();
        let (cursor, values) = page(params.execute(Some(entry.clone())));
        assert_ne!(cursor, 0);
        assert!(values.is_empty());
        let mut params = parse(&[
            b"ZSCAN",
            b"z",
            b"0",
            b"MATCH",
            b"member:2??",
            b"COUNT",
            b"5",
        ])
        .unwrap();
        let mut actual = HashSet::new();
        for _ in 0..1024 {
            let (cursor, values) = page(params.execute(Some(entry.clone())));
            actual.extend(values.chunks_exact(2).map(|pair| pair[0].clone()));
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0);
        assert_eq!(
            actual,
            (200..256)
                .map(|i| Bytes::from(format!("member:{i}")))
                .collect()
        );
    }

    #[test]
    fn reads_filter_expiration_and_real_writes_between_pages_keep_stable_members() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let stable: HashSet<_> = (0..128)
            .map(|i| Bytes::from(format!("stable:{i}")))
            .collect();
        let set = sorted_set(stable.iter().map(|member| (member.clone(), 1.0)));
        cache.databases[0].mocha.insert_absolute(
            "z".into(),
            MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(set)))),
            200,
        );
        let mut params = parse(&[b"ZSCAN", b"z", b"0", b"COUNT", b"1"]).unwrap();
        for clock in [200, 201] {
            assert_eq!(
                page(read_request(
                    &cache,
                    ReadOperation::ZScan(params.clone()),
                    0,
                    clock
                )),
                (0, Vec::new())
            );
            assert_eq!(
                page(cache.execute_read(params.clone(), 0, clock)),
                (0, Vec::new())
            );
        }
        assert_eq!(cache.databases[0].mocha.len(), 1);
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: 100,
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let temporary: Vec<_> = (0..1024)
            .map(|i| Bytes::from(format!("temporary:{i}")))
            .collect();
        let mut seen = HashSet::new();
        for step in 0..4096 {
            let (cursor, values) = page(read_request(
                &cache,
                ReadOperation::ZScan(params.clone()),
                0,
                199,
            ));
            seen.extend(values.chunks_exact(2).map(|pair| pair[0].clone()));
            if step == 0 {
                assert_ne!(cursor, 0);
                cache.execute_compute(
                    add(temporary
                        .iter()
                        .map(|member| (member.clone(), 2.0))
                        .collect()),
                    &mut update,
                );
                // Updating scores must not move stable members between scan buckets.
                cache.execute_compute(
                    add(stable.iter().map(|member| (member.clone(), 3.0)).collect()),
                    &mut update,
                );
            } else if step <= temporary.len() {
                cache.execute_compute(
                    crate::protocol::zset::zrem::ZRemReq {
                        key: "z".into(),
                        members: vec![temporary[step - 1].clone()],
                    },
                    &mut update,
                );
            }
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0);
        assert!(stable.is_subset(&seen));
    }

    #[test]
    fn ordinary_cursor_is_deterministic_in_exec_and_lua_after_restore() {
        use crate::protocol::lua::eval::EvalParams;
        use crate::protocol::raft_command::RaftCommandFactory;
        use crate::protocol::string::get::GetParams;
        use crate::protocol::transaction::{QueuedOperation, exec::ExecParams};
        use crate::raft::types::entry::request::{Operation, RedisOperation, Request};

        let caches: Vec<_> = (0..3)
            .map(|replica| {
                let cache = MyCache::new(1).unwrap();
                cache.pause_expire_workers();
                cache.set_write_clock(100);
                let mut members: Vec<_> = (0..128)
                    .map(|i| (Bytes::from(format!("member:{i}")), i as f64))
                    .collect();
                if replica != 0 {
                    members.reverse();
                }
                let mut value =
                    MyValue::new(ValueObject::ZSet(Arc::new(Mutex::new(sorted_set(members)))));
                if replica == 2 {
                    value = bincode2::deserialize(&bincode2::serialize(&value).unwrap()).unwrap();
                }
                cache.databases[0]
                    .mocha
                    .insert_absolute("z".into(), value, 1000);
                cache
            })
            .collect();
        let args = arguments(&[b"ZSCAN", b"z", b"0", b"COUNT", b"1"]);
        let factory = RaftCommandFactory::init_lua();
        let Operation::Read(read) = factory.parse_request(&args).unwrap() else {
            panic!("ZSCAN must be a read");
        };
        let (cursor, _) = page(read_request(&caches[0], read, 0, 199));
        assert_ne!(cursor, 0);
        let mut continuation = args;
        continuation[2] = Value::BulkString(Some(cursor.to_string().into()));
        let scan = factory.parse_request(&continuation).unwrap();
        for proto in [2, 3] {
            let mut script = EvalParams::new(
                Bytes::from_static(
                    br#"
                local result = redis.call('ZSCAN', KEYS[1], ARGV[1], 'COUNT', 1)
                redis.call('SET', KEYS[2], result[2][1])
                redis.call('ZADD', KEYS[1], 42, result[2][1])
                return result
            "#,
                ),
                2,
                vec!["z".into(), "scan:first".into()],
                vec![cursor.to_string().into()],
            );
            script.proto = proto;
            let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    QueuedOperation::new(0, scan.clone()),
                    QueuedOperation::new(0, Operation::Redis(RedisOperation::RedisEval(script))),
                    QueuedOperation::new(0, scan.clone()),
                ],
            }));
            let log = bincode2::serialize(&Request::new(199, 0, operation)).unwrap();
            let replies: Vec<_> = caches
                .iter()
                .map(|cache| {
                    let request: Request = bincode2::deserialize(&log).unwrap();
                    let (clock, db_number) = request.split_u64();
                    let mut update_type = UpdateType::None;
                    let mut update = Update {
                        db_number,
                        write_clock: cache.set_write_clock(clock),
                        update_type: &mut update_type,
                        effects: None,
                        waiters: None,
                    };
                    do_request(cache, request.operation, &mut update, true)
                })
                .collect();
            for (cache, reply) in caches.iter().zip(&replies) {
                assert_eq!(reply.encode_proto(proto), replies[0].encode_proto(proto));
                let Value::Array(Some(results)) = reply else {
                    panic!("expected EXEC reply, got {reply:?}");
                };
                assert_eq!(results.len(), 3);
                for result in results {
                    let (cursor, values) = page(result.clone());
                    assert_eq!(cursor, 0);
                    assert_eq!(values.len(), 256);
                    assert!(values.chunks_exact(2).map(|pair| &pair[0]).is_sorted());
                }
                assert_eq!(
                    cache
                        .execute_read(
                            GetParams {
                                key: "scan:first".into()
                            },
                            0,
                            199
                        )
                        .string_bytes_clone()
                        .unwrap()
                        .as_ref(),
                    b"member:0"
                );
            }
        }
    }
}
