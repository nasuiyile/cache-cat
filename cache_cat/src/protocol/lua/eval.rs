use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::{RedisEval, RedisReply};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for EVAL command
///
/// Standard Redis EVAL command format:
/// EVAL script numkeys key [key ...] arg [arg ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalParams {
    /// The Lua script to execute
    pub script: Bytes,
    /// Number of keys
    pub numkeys: usize,
    /// Key names
    pub keys: Vec<Bytes>,
    /// Arguments
    pub args: Vec<Bytes>,
    /// RESP protocol version of the calling client (2 or 3). Controls the
    /// Lua -> RESP conversion of the script's return value, like Redis.
    #[serde(default = "default_eval_proto")]
    pub proto: u8,
}

#[inline]
pub(crate) fn default_eval_proto() -> u8 {
    2
}

impl Display for EvalParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EVAL {}{} ({} keys, {} args)",
            String::from_utf8_lossy(&self.script[..self.script.len().min(20)]),
            if self.script.len() > 20 { "..." } else { "" },
            self.numkeys,
            self.args.len()
        )
    }
}

impl EvalParams {
    /// Create a new EvalParams
    pub fn new(script: Bytes, numkeys: usize, keys: Vec<Bytes>, args: Vec<Bytes>) -> Self {
        Self {
            script,
            numkeys,
            keys,
            args,
            proto: default_eval_proto(),
        }
    }

    /// Parse EVAL command parameters from RESP array items
    /// Format: EVAL script numkeys key [key ...] arg [arg ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: EVAL script numkeys
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("eval"));
        }

        // Parse script
        let script = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("script"))?;

        let remaining = items.len() - 3;
        // Redis parses this as a signed, canonical integer first.  Parsing it
        // directly as usize would accept forms such as `+1`/`01` and would
        // return the generic integer error for negative values instead of the
        // command-specific Redis response.  Keep the range check before any
        // slice conversion so an untrusted value cannot influence indexing.
        let numkeys = parse_numkeys(&items[2], remaining)?;

        // Parse keys
        let keys = items[3..3 + numkeys]
            .iter()
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() < numkeys {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        let start = 3 + numkeys;
        // Parse arguments (remaining items after keys)
        let args = items
            .iter()
            .skip(start)
            .map_while(|arg_value| match arg_value {
                Value::BulkString(Some(data)) => Some(data.clone()),
                Value::SimpleString(s) => Some(s.clone().into()),
                Value::Integer(i) => Some(i.to_string().into()),
                _ => None,
            })
            .collect::<Vec<_>>();

        if args.len() < items.len() - start {
            return Err(ProtocolError::InvalidArgument("argument"));
        }

        Ok(EvalParams::new(script, numkeys, keys, args))
    }
}

/// Parse the EVAL/EVALSHA `numkeys` argument with Redis' exact validation and
/// error responses.  The caller supplies the number of arguments following
/// `numkeys`; this lets us reject an oversized count without allocating or
/// constructing a temporary key/argument vector.
#[inline]
pub(crate) fn parse_numkeys(value: &Value, remaining: usize) -> Result<usize, ProtocolError> {
    let parsed = value.try_parse_canonical_i64()?;
    if parsed < 0 {
        return Err(ProtocolError::response(
            "ERR Number of keys can't be negative",
        ));
    }

    // Compare in u64 before converting to usize.  Redis uses a signed
    // long-long for this field; this remains correct on 32-bit targets too.
    let parsed = parsed as u64;
    if parsed > remaining as u64 {
        return Err(ProtocolError::response(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    Ok(parsed as usize)
}

/// Redis validates these command arguments at execution time. Cache their
/// deterministic reply in MULTI so an invalid count does not abort the whole
/// transaction. Arity and non-string protocol errors still fail immediately.
#[cold]
pub(crate) fn eval_parse_error(
    client: &mut Client,
    error: ProtocolError,
) -> Result<Value, CacheCatError> {
    if matches!(
        error,
        ProtocolError::NotAnInteger | ProtocolError::Response(_)
    ) {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(Operation::Redis(RedisReply(error.into())));
            return Ok(Value::queued());
        }
    }
    Err(error.into())
}

/// EVAL command executor
pub struct EvalCommand;

impl RaftCommand for EvalCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = EvalParams::parse(items)?;
        Ok(Operation::Redis(RedisEval(params)))
    }
}

#[async_trait]
impl Command for EvalCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut operation = match self.raft_request(items) {
            Ok(operation) => operation,
            Err(error) => return eval_parse_error(client, error),
        };
        if let Operation::Redis(RedisEval(ref mut params)) = operation {
            params.proto = client.framed.codec().proto_version();
        }
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(operation);
            return Ok(Value::queued());
        }
        let result = server.app.write(operation, client.db_number).await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::node::raft_node::RaftNode;
    use crate::protocol::common::resp::Parser;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::protocol::transaction::multi::MultiCommand;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::do_request;
    use crate::raft::types::entry::request::RedisOperation;
    use bytes::BytesMut;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn queued_eval_preserves_client_response_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.raft.log_path = dir.path().to_str().unwrap().to_owned();
        config.raft.address = "127.0.0.1:0".into();
        config.redis.databases = 1;
        let config = ParsedConfig::from(&config).unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let node = RaftNode::create(config.clone(), shutdown_tx).await.unwrap();
        let server = RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (connection, _) = listener.accept().await.unwrap();
        let mut client = Client::new(1, connection, true);

        for proto in [2, 3] {
            if proto == 3 {
                client.framed.codec_mut().switch_resp3();
            }
            MultiCommand
                .execute(&mut client, &[Value::SimpleString("MULTI".into())], &server)
                .await
                .unwrap();
            for script in ["return true", "return false"] {
                let items = ["EVAL", script, "0"]
                    .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part.as_bytes()))));
                let reply = EvalCommand
                    .execute(&mut client, &items, &server)
                    .await
                    .unwrap();
                assert_eq!(reply.encode(), b"+QUEUED\r\n");
            }
            let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: client.transaction_queue.take().unwrap().operations,
            }));
            let encoded = bincode2::serialize(&operation).unwrap();
            let operation: Operation = bincode2::deserialize(&encoded).unwrap();
            let mut update_type = UpdateType::None;
            let mut update = Update {
                db_number: 0,
                write_clock: 0,
                update_type: &mut update_type,
            };
            let reply = do_request(
                &node.app.state_machine.data.kvs,
                operation,
                &mut update,
                true,
            );
            let expected = if proto == 3 {
                b"*2\r\n#t\r\n#f\r\n".as_slice()
            } else {
                b"*2\r\n:1\r\n$-1\r\n".as_slice()
            };
            assert_eq!(reply.encode_proto(proto), expected);
        }
        node.app.cluster.shutdown().await.unwrap();
    }

    fn execute(cache: &MyCache, parts: &[&[u8]]) -> Value {
        execute_proto(cache, parts, 2)
    }

    fn execute_proto(cache: &MyCache, parts: &[&[u8]], proto: u8) -> Value {
        let items: Vec<_> = parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part))))
            .collect();
        let mut operation = EvalCommand.raft_request(&items).unwrap();
        let Operation::Redis(RedisEval(params)) = &mut operation else {
            panic!("EVAL did not produce a replicated script operation");
        };
        params.proto = proto;
        // Exercise the same serialization used for replicated operations.
        let encoded = bincode2::serialize(&operation).unwrap();
        let operation: Operation = bincode2::deserialize(&encoded).unwrap();
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: 0,
            update_type: &mut update_type,
        };
        do_request(cache, operation, &mut update, true)
    }

    fn assert_followed_by_pong(wire: &[u8], proto: u8) {
        let mut pipeline = BytesMut::from(wire);
        pipeline.extend_from_slice(b"+PONG\r\n");
        let reply = Parser::take_from_bytes_stream(&mut pipeline)
            .unwrap()
            .expect("the EVAL reply must be a complete frame");
        assert_eq!(reply.encode_proto(proto), wire);
        assert_eq!(pipeline.as_ref(), b"+PONG\r\n");
        assert!(matches!(
            Parser::take_from_bytes_stream(&mut pipeline).unwrap(),
            Some(Value::SimpleString(s)) if s == "PONG"
        ));
        assert!(pipeline.is_empty());
        assert!(
            Parser::take_from_bytes_stream(&mut pipeline)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn eval_numkeys_matches_redis_validation() {
        let request = |numkeys: &[u8], extra: &[&[u8]]| {
            let mut parts = vec![
                Value::BulkString(Some(Bytes::from_static(b"EVAL"))),
                Value::BulkString(Some(Bytes::from_static(b"return 1"))),
                Value::BulkString(Some(Bytes::copy_from_slice(numkeys))),
            ];
            parts.extend(
                extra
                    .iter()
                    .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part)))),
            );
            parts
        };

        let invalid = [
            (
                b"-1".as_slice(),
                &[][..],
                b"ERR Number of keys can't be negative".as_slice(),
            ),
            (
                b"+1".as_slice(),
                &[][..],
                b"ERR value is not an integer or out of range".as_slice(),
            ),
            (
                b"01".as_slice(),
                &[][..],
                b"ERR value is not an integer or out of range".as_slice(),
            ),
            (
                b"1 ".as_slice(),
                &[][..],
                b"ERR value is not an integer or out of range".as_slice(),
            ),
            (
                b"1".as_slice(),
                &[][..],
                b"ERR Number of keys can't be greater than number of args".as_slice(),
            ),
        ];
        for (numkeys, extra, expected) in invalid {
            let items = request(numkeys, extra);
            let error = EvalCommand
                .raft_request(&items)
                .expect_err("invalid EVAL numkeys must be rejected");
            assert_eq!(error.to_string(), String::from_utf8_lossy(expected));
        }

        let items = request(b"1", &[b"key"]);
        let operation = EvalCommand.raft_request(&items);
        assert!(operation.is_ok());
    }

    #[test]
    fn eval_lua_status_and_error_replies_preserve_frame_boundaries() {
        let cache = MyCache::new(1).unwrap();
        let cases: &[(&[u8], &[u8])] = &[
            (br"return {ok='OK\r\n:123'}", b"+OK  :123\r\n"),
            (br"return {err='ERR bad\r\n:123'}", b"-ERR bad  :123\r\n"),
            (
                br"return redis.status_reply('OK\r\n:123')",
                b"+OK  :123\r\n",
            ),
            (
                br"return redis.error_reply('ERR bad\r\n:123')",
                b"-ERR bad  :123\r\n",
            ),
            (
                br"return {ok='\rOK\nleft\rright\r\n'}",
                b"+ OK left right  \r\n",
            ),
            (
                br"return {err='\rERR bad\nleft\rright\r\n'}",
                b"- ERR bad left right\r\n",
            ),
            (br"return {ok='OK\000hidden\r\n:123'}", b"+OK\r\n"),
            (
                br"return {err='ERR bad\000hidden\r\n:123'}",
                b"-ERR bad\r\n",
            ),
            (
                br"return redis.status_reply('OK\000hidden\r\n:123')",
                b"+OK\r\n",
            ),
            (
                br"return redis.error_reply('ERR bad\000hidden\r\n:123')",
                b"-ERR bad\r\n",
            ),
            (b"return {ok='OK'}", b"+OK\r\n"),
            (b"return {err='ERR bad'}", b"-ERR bad\r\n"),
        ];
        for proto in [2, 3] {
            for &(script, expected) in cases {
                let wire =
                    execute_proto(&cache, &[b"EVAL", script, b"0"], proto).encode_proto(proto);
                assert_eq!(wire, expected, "RESP{proto}, script {script:?}");
                assert_followed_by_pong(&wire, proto);
            }
        }
    }

    #[test]
    fn lua_error_reply_matches_redis_error_code_rules() {
        let cache = MyCache::new(1).unwrap();
        // Check the Lua-visible field separately: interior CR/LF must not be
        // sanitized until the table is converted into a RESP error reply.
        let cases: &[(&[u8], &[u8])] = &[
            (b"hello", b"ERR hello"),
            (b"some text", b"some text"),
            (b"Bad-Code details", b"Bad-Code details"),
            (b"-WRONGTYPE details", b"WRONGTYPE details"),
            (b"--CODE details", b"-CODE details"),
            (b"-hello", b"ERR hello"),
            (b"", b"ERR "),
            (b"-", b"ERR "),
            (b" details", b" details"),
            (b" ", b" "),
            (b"ERR  details ", b"ERR  details "),
            (b"bad\tmessage", b"ERR bad\tmessage"),
            (b"-CODE \r\ndetails\r\n", b"CODE details"),
            (b"\r\ndetails\r\n", b"ERR details"),
            (b"CODE \r\n", b"CODE "),
            (b"CODE a\r\n:123", b"CODE a\r\n:123"),
            (b"low\r\ncode details", b"low\r\ncode details"),
            (b"hello\0ignored code", b"ERR hello"),
            (b"-CODE details\0ignored", b"CODE details"),
            (b"\0ignored", b"ERR "),
        ];
        for proto in [2, 3] {
            for &(input, expected) in cases {
                let field = execute_proto(
                    &cache,
                    &[
                        b"EVAL",
                        b"return redis.error_reply(ARGV[1]).err",
                        b"0",
                        input,
                    ],
                    proto,
                );
                assert!(matches!(field, Value::BulkString(Some(ref bytes)) if bytes == expected));

                let reply = execute_proto(
                    &cache,
                    &[b"EVAL", b"return redis.error_reply(ARGV[1])", b"0", input],
                    proto,
                );
                let mut expected_wire = vec![b'-'];
                expected_wire.extend(expected.iter().map(|&byte| match byte {
                    b'\r' | b'\n' => b' ',
                    byte => byte,
                }));
                expected_wire.extend_from_slice(b"\r\n");
                let wire = reply.encode_proto(proto);
                assert_eq!(wire, expected_wire, "RESP{proto}: {input:?}");
                assert_followed_by_pong(&wire, proto);
            }
        }

        let field = execute(
            &cache,
            &[
                b"EVAL",
                b"return redis.error_reply(ARGV[1]).err",
                b"0",
                b"code \xff\r\n:123",
            ],
        );
        assert!(
            matches!(field, Value::BulkString(Some(bytes)) if bytes == b"code \xff\r\n:123".as_slice())
        );
    }

    #[test]
    fn eval_nested_replies_preserve_frames_and_binary_bulk_values() {
        let cache = MyCache::new(1).unwrap();
        let script = br"return {{ok='OK\r\n:123'}, {{err='ERR bad\r\n:456'}, ARGV[1]}}";
        let payload = b"\0\xff\r\n:789\r\n\xfe\n\r";
        let mut expected = b"*2\r\n+OK  :123\r\n*2\r\n-ERR bad  :456\r\n".to_vec();
        expected
            .extend_from_slice(&Value::BulkString(Some(Bytes::copy_from_slice(payload))).encode());
        for proto in [2, 3] {
            let wire =
                execute_proto(&cache, &[b"EVAL", script, b"0", payload], proto).encode_proto(proto);
            assert_eq!(wire, expected);
            assert_followed_by_pong(&wire, proto);

            let wire = execute_proto(&cache, &[b"EVAL", b"return ARGV[1]", b"0", payload], proto)
                .encode_proto(proto);
            assert_eq!(
                wire,
                Value::BulkString(Some(Bytes::copy_from_slice(payload))).encode()
            );
            assert_followed_by_pong(&wire, proto);
        }
    }

    #[test]
    fn eval_big_number_replies_preserve_frame_boundaries() {
        let cache = MyCache::new(1).unwrap();
        let payload = b"123\r\n:456\0tail\n\r";
        let sanitized = b"123  :456\0tail  ";
        for proto in [2, 3] {
            let wire = execute_proto(
                &cache,
                &[b"EVAL", b"return {big_number=ARGV[1]}", b"0", payload],
                proto,
            )
            .encode_proto(proto);
            let expected = if proto == 3 {
                let mut wire = vec![b'('];
                wire.extend_from_slice(sanitized);
                wire.extend_from_slice(b"\r\n");
                wire
            } else {
                Value::BulkString(Some(Bytes::copy_from_slice(sanitized))).encode()
            };
            assert_eq!(wire, expected);
            if proto == 2 {
                assert_followed_by_pong(&wire, proto);
            } else {
                // Redis sanitizes the Lua string without validating digits.
                // Our reply parser accepts only numeric big-number payloads,
                // so check the malformed payload's wire boundary directly.
                assert!(!wire[..wire.len() - 2].contains(&b'\r'));
                assert!(!wire[..wire.len() - 2].contains(&b'\n'));
            }

            let wire = execute_proto(
                &cache,
                &[
                    b"EVAL",
                    b"return {big_number='123456789012345678901234567890'}",
                    b"0",
                ],
                proto,
            )
            .encode_proto(proto);
            assert_followed_by_pong(&wire, proto);
        }
    }

    #[test]
    fn eval_runtime_and_compile_errors_preserve_frame_boundaries() {
        let cache = MyCache::new(1).unwrap();
        let cases: &[(&[u8], &[u8])] = &[
            (
                br"error('bad\r\n:123\rleft\nright')",
                b"bad  :123 left right",
            ),
            (
                b"return 'unterminated\r\n:123",
                b"ERR Error compiling script:",
            ),
        ];
        for proto in [2, 3] {
            for &(script, expected_message) in cases {
                let reply = execute_proto(&cache, &[b"EVAL", script, b"0"], proto);
                assert!(matches!(reply, Value::Error(_)), "{reply:?}");
                let wire = reply.encode_proto(proto);
                assert_eq!(wire[0], b'-');
                assert!(wire.ends_with(b"\r\n"));
                assert!(!wire[..wire.len() - 2].contains(&b'\r'), "{wire:?}");
                assert!(!wire[..wire.len() - 2].contains(&b'\n'), "{wire:?}");
                assert!(
                    wire.windows(expected_message.len())
                        .any(|part| part == expected_message),
                    "{wire:?}"
                );
                assert_followed_by_pong(&wire, proto);
            }
        }
    }

    #[test]
    fn lua_calls_preserve_binary_keys_and_values() {
        let cache = MyCache::new(1).unwrap();
        let key = b"key\xff\0\r\n";
        let value = b"value\xfe\0\r\n";
        for function in ["call", "pcall"] {
            let script = format!(
                "redis.{function}('SET', KEYS[1], ARGV[1]); return redis.{function}('GET', KEYS[1])"
            );
            assert_eq!(
                execute(&cache, &[b"EVAL", script.as_bytes(), b"1", key, value]).encode(),
                Value::BulkString(Some(Bytes::copy_from_slice(value))).encode()
            );
            // Lua numbers must still be accepted as command arguments.
            let script = format!(
                "redis.{function}('SET', KEYS[1], 123); return redis.{function}('GET', KEYS[1])"
            );
            assert_eq!(
                execute(&cache, &[b"EVAL", script.as_bytes(), b"1", key]).encode(),
                b"$3\r\n123\r\n"
            );
        }
        assert_eq!(
            execute(&cache, &[b"EVAL", b"return ARGV[1]", b"0", value]).encode(),
            Value::BulkString(Some(Bytes::copy_from_slice(value))).encode()
        );
    }

    #[test]
    fn eval_preserves_binary_source_in_replication_and_compiled_cache() {
        let cache = MyCache::new(1).unwrap();
        for byte in [0xff, 0xfe, 0xff] {
            let mut script = b"return \"".to_vec();
            script.extend_from_slice(&[byte, b'"']);
            assert_eq!(
                execute(&cache, &[b"EVAL", &script, b"0"]).encode(),
                vec![b'$', b'1', b'\r', b'\n', byte, b'\r', b'\n']
            );
        }
    }

    #[test]
    fn eval_rejects_precompiled_lua_bytecode() {
        let cache = MyCache::new(1).unwrap();
        let bytecode = mlua::Lua::new()
            .load("return 123")
            .into_function()
            .unwrap()
            .dump(false);
        assert!(matches!(
            execute(&cache, &[b"EVAL", &bytecode, b"0"]),
            Value::Error(_)
        ));
    }
}
