//! SSCAN key cursor [MATCH pattern] [COUNT count].

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::common::scan::parse_cursor;
use crate::protocol::option_eq;
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::glob::GlobMatcher;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SScanParams {
    pub key: Bytes,
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    // Redis validates the cursor before lookup, but options after checking
    // that the key exists and has the expected type, including in EXEC/Lua.
    option_error: Option<String>,
}

impl Display for SScanParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SSCAN {} {} COUNT {}",
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

impl ReadCommand for SScanParams {
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

impl SScanParams {
    fn execute_scan<const DETERMINISTIC: bool>(
        &self,
        value: Option<EntrySnapshot<MyValue>>,
    ) -> Value {
        let Some(snapshot) = value else {
            return Self::reply(0, Vec::new());
        };
        let ValueObject::Set(set) = snapshot.value.data else {
            return ProtocolError::WrongType.into();
        };
        if let Some(error) = &self.option_error {
            return Value::error(error.clone());
        }
        let guard = set.lock();
        let matcher = self.pattern.as_deref().map(GlobMatcher::new);
        let matches = |member: &Bytes| {
            matcher
                .as_ref()
                .is_none_or(|pattern| pattern.matches(member))
        };
        let mut result = Vec::with_capacity(self.count.min(guard.len()).min(256));

        if DETERMINISTIC {
            // Hash seeds, bucket capacities and scan cursors are node-local.
            // Replicated reads return all matches in member order and finish
            // the scan, accepting a cursor from an earlier ordinary read.
            let mut entries: Vec<_> = guard.iter().filter(|member| matches(member)).collect();
            entries.sort_unstable();
            result.extend(
                entries
                    .into_iter()
                    .map(|member| Value::BulkString(Some(member.clone()))),
            );
            return Self::reply(0, result);
        }

        let cursor = guard.scan(self.cursor, self.count, |member| {
            if matches(member) {
                result.push(Value::BulkString(Some(member.clone())));
            }
        });
        Self::reply(cursor, result)
    }

    fn reply(cursor: u64, entries: Vec<Value>) -> Value {
        Value::Array(Some(vec![
            Value::BulkString(Some(cursor.to_string().into())),
            Value::Array(Some(entries)),
        ]))
    }
}

pub struct SScanCommand;

impl SScanCommand {
    fn parse_args(items: &[Value]) -> Result<SScanParams, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("sscan"));
        }
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;
        let cursor = items[2]
            .string_bytes_clone()
            .and_then(|value| parse_cursor(&value))
            .ok_or(ProtocolError::InvalidArgument("cursor"))?;
        let mut params = SScanParams {
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

    fn parse_options(params: &mut SScanParams, items: &[Value]) -> Result<(), ProtocolError> {
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

impl ReadRaftCommand for SScanCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::SScan(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for SScanCommand {
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
    use crate::protocol::set::sadd::SAddReq;
    use crate::protocol::set::srem::SRemReq;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::{do_request, read_request};
    use crate::raft::types::core::structure::scan_set::ScanSet;
    use parking_lot::Mutex;
    use std::collections::{BTreeSet, HashSet};
    use std::sync::Arc;

    fn arguments(args: &[&[u8]]) -> Vec<Value> {
        args.iter()
            .map(|arg| Value::BulkString(Some(Bytes::copy_from_slice(arg))))
            .collect()
    }

    fn parse(args: &[&[u8]]) -> Result<SScanParams, ProtocolError> {
        SScanCommand::parse_args(&arguments(args))
    }

    fn add(elements: Vec<Bytes>) -> SAddReq {
        SAddReq {
            key: "s".into(),
            elements,
        }
    }

    fn snapshot(set: ScanSet<Bytes>) -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
            expire_at: None,
        }
    }

    fn page(reply: Value) -> (u64, Vec<Bytes>) {
        let Value::Array(Some(mut outer)) = reply else {
            panic!("expected scan array, got {reply:?}");
        };
        assert_eq!(outer.len(), 2);
        let Value::Array(Some(values)) = outer.pop().unwrap() else {
            panic!("expected member array");
        };
        let Value::BulkString(Some(cursor)) = outer.pop().unwrap() else {
            panic!("expected bulk string cursor");
        };
        let cursor = std::str::from_utf8(&cursor).unwrap().parse().unwrap();
        (
            cursor,
            values
                .into_iter()
                .map(|value| {
                    let Value::BulkString(Some(value)) = value else {
                        panic!("SSCAN members must be bulk strings");
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
            assert_eq!(parse(&[b"SSCAN", b"s", input]).unwrap().cursor, expected);
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
                parse(&[b"SSCAN", b"s", input]).unwrap_err(),
                ProtocolError::InvalidArgument("cursor")
            );
        }
        assert_eq!(
            parse(&[b"SSCAN", b"s"]).unwrap_err(),
            ProtocolError::WrongArgCount("sscan")
        );
        let params = parse(&[
            b"SSCAN",
            b"s",
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
            (vec![b"TYPE".as_slice(), b"set"], ProtocolError::SyntaxError),
            (
                vec![b"NOVALUES".as_slice()],
                ProtocolError::response("ERR NOVALUES option can only be used in HSCAN"),
            ),
        ] {
            let mut args = vec![b"SSCAN".as_slice(), b"s", b"0"];
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
                params.execute(Some(snapshot(ScanSet::new()))).encode(),
                Value::from(error).encode()
            );
            let restored: SScanParams =
                bincode2::deserialize(&bincode2::serialize(&params).unwrap()).unwrap();
            assert_eq!(restored.option_error, params.option_error);
        }
    }

    #[test]
    fn replies_remain_arrays_in_resp2_and_resp3() {
        let params = parse(&[b"SSCAN", b"s", b"0"]).unwrap();
        let entry = snapshot([Bytes::from_static(b"a")].into_iter().collect());
        let reply = params.execute(Some(entry));
        for proto in [2, 3] {
            assert_eq!(
                reply.encode_proto(proto).as_slice(),
                b"*2\r\n$1\r\n0\r\n*1\r\n$1\r\na\r\n"
            );
            for value in [None, Some(snapshot(ScanSet::new()))] {
                assert_eq!(
                    params.execute(value).encode_proto(proto).as_slice(),
                    b"*2\r\n$1\r\n0\r\n*0\r\n"
                );
            }
        }
    }

    #[test]
    fn full_scan_covers_binary_members() {
        let mut expected: BTreeSet<_> = (0..257)
            .map(|i| Bytes::from(format!("member:{i}")))
            .collect();
        expected.insert(Bytes::new());
        expected.insert(Bytes::from_static(b"\0\xff"));
        let entry = snapshot(expected.iter().cloned().collect());
        let mut params = parse(&[b"SSCAN", b"s", b"0", b"COUNT", b"3"]).unwrap();
        let mut actual = BTreeSet::new();
        let mut calls = 0;
        loop {
            let reply = params.execute(Some(entry.clone()));
            assert_eq!(reply.encode_proto(2), reply.encode_proto(3));
            let (cursor, values) = page(reply);
            for member in values {
                assert!(actual.insert(member));
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
    fn match_filters_candidates_after_count_and_supports_binary_patterns() {
        let entry = snapshot(
            (0..256)
                .map(|i| Bytes::from(format!("member:{i}")))
                .collect(),
        );
        let params =
            parse(&[b"SSCAN", b"s", b"0", b"MATCH", b"missing:*", b"COUNT", b"1"]).unwrap();
        let (cursor, values) = page(params.execute(Some(entry.clone())));
        assert_ne!(cursor, 0);
        assert!(values.is_empty());
        let mut params = parse(&[
            b"SSCAN",
            b"s",
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
            actual.extend(values);
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

        let binary = snapshot(
            [b"\0\xff*".as_slice(), b"\0\xff?", b"\0\xffx", b"\0xx*"]
                .into_iter()
                .map(Bytes::copy_from_slice)
                .collect(),
        );
        let params = parse(&[
            b"SSCAN",
            b"s",
            b"0",
            b"MATCH",
            b"\0[\xff]\\*",
            b"COUNT",
            b"100",
        ])
        .unwrap();
        assert_eq!(
            page(params.execute(Some(binary.clone()))),
            (0, vec![Bytes::from_static(b"\0\xff*")])
        );
        assert_eq!(
            page(params.execute_with_clock(Some(binary), 123)),
            (0, vec![Bytes::from_static(b"\0\xff*")])
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
        cache.databases[0].mocha.insert_absolute(
            "s".into(),
            snapshot(stable.iter().cloned().collect()).value,
            200,
        );
        let mut params = parse(&[b"SSCAN", b"s", b"0", b"COUNT", b"1"]).unwrap();
        for clock in [200, 201] {
            assert_eq!(
                page(read_request(
                    &cache,
                    ReadOperation::SScan(params.clone()),
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
                ReadOperation::SScan(params.clone()),
                0,
                199,
            ));
            seen.extend(values);
            if step == 0 {
                assert_ne!(cursor, 0);
                assert_eq!(
                    cache
                        .execute_compute(add(temporary.clone()), &mut update)
                        .parse_canonical_i64(),
                    Some(1024)
                );
                assert_eq!(
                    cache
                        .execute_compute(add(stable.iter().cloned().collect()), &mut update)
                        .parse_canonical_i64(),
                    Some(0)
                );
            } else if step <= temporary.len() {
                assert_eq!(
                    cache
                        .execute_compute(
                            SRemReq {
                                key: "s".into(),
                                members: vec![temporary[step - 1].clone()],
                            },
                            &mut update,
                        )
                        .parse_canonical_i64(),
                    Some(1)
                );
            }
            params.cursor = cursor;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(params.cursor, 0);
        assert!(stable.is_subset(&seen));
        assert_eq!(
            page(read_request(&cache, ReadOperation::SScan(params), 0, 200)),
            (0, Vec::new())
        );
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
                    .map(|i| Bytes::from(format!("member:{i}")))
                    .collect();
                if replica != 0 {
                    members.reverse();
                }
                let mut set = ScanSet::with_capacity(128 << (replica * 2));
                set.extend(members);
                let mut value = snapshot(set).value;
                if replica == 2 {
                    value = bincode2::deserialize(&bincode2::serialize(&value).unwrap()).unwrap();
                }
                cache.databases[0]
                    .mocha
                    .insert_absolute("s".into(), value, 1000);
                cache
            })
            .collect();
        let args = arguments(&[b"SSCAN", b"s", b"0", b"COUNT", b"1"]);
        let factory = RaftCommandFactory::init_lua();
        let Operation::Read(read) = factory.parse_request(&args).unwrap() else {
            panic!("SSCAN must be a read");
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
                local result = redis.call('SSCAN', KEYS[1], ARGV[1], 'COUNT', 1)
                redis.call('SET', KEYS[2], result[2][1])
                redis.call('SREM', KEYS[1], result[2][1])
                redis.call('SADD', KEYS[1], result[2][1])
                return result
            "#,
                ),
                2,
                vec!["s".into(), "scan:first".into()],
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
                    assert_eq!(values.len(), 128);
                    assert!(values.iter().is_sorted());
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
