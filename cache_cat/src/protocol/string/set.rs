use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::option_eq;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisSet;
use crate::utils::parse_canonical_i64;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// Expiration time options for SET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expiration {
    /// EX seconds - Set the specified expire time, in seconds
    Ex(u64),
    /// PX milliseconds - Set the specified expire time, in milliseconds
    Px(u64),
    /// EXAT timestamp-seconds - Set the specified Unix time at which the key will expire, in seconds
    ExAt(u64),
    /// PXAT timestamp-milliseconds - Set the specified Unix time at which the key will expire, in milliseconds
    PxAt(u64),
    /// KEEPTTL - Retain the time to live associated with the key
    KeepTTL,
}

/// Set mode options (NX/XX)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetMode {
    /// NX - Only set the key if it does not already exist
    Nx,
    /// XX - Only set the key if it already exists
    Xx,
}

/// Parameters for SET command
///
/// Standard Redis SET command format:
/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetParams {
    /// The key to set
    pub key: Bytes,
    /// The value to set
    pub value: Bytes,
    /// NX or XX mode (optional)
    pub mode: Option<SetMode>,
    /// Whether to return the previous value (GET option)
    pub get: bool,
    /// Expiration time options (optional)
    pub expiration: Option<Expiration>,
}

impl Display for SetParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SET {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

impl SetParams {
    /// Create a new SetParams with minimal required fields
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            mode: None,
            get: false,
            expiration: None,
        }
    }

    fn invalid_expire_time() -> ProtocolError {
        ProtocolError::response("ERR invalid expire time in 'set' command")
    }

    fn parse_positive_expiration(value: &Value, seconds: bool) -> Result<u64, ProtocolError> {
        let value = value.try_parse_canonical_i64()?;
        if value <= 0 {
            return Err(Self::invalid_expire_time());
        }
        if seconds && value.checked_mul(1000).is_none() {
            return Err(Self::invalid_expire_time());
        }
        Ok(value as u64)
    }

    /// Parse SET command parameters from RESP array items
    /// Format: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: SET key value
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("set"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let value = items[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("value"))?;

        let mut params = SetParams::new(key, value);
        let mut expiration_value = None;
        let mut i = 3;

        // Parse optional arguments
        while i < items.len() {
            let arg: &[u8] = match &items[i] {
                Value::BulkString(Some(data)) | Value::VerbatimString { data, .. } => data,
                Value::SimpleString(value) => value.as_bytes(),
                _ => return Err(ProtocolError::SyntaxError),
            };

            match arg {
                arg if option_eq(arg, b"NX") => {
                    if matches!(params.mode, Some(SetMode::Xx)) {
                        return Err(ProtocolError::SyntaxError);
                    }
                    params.mode = Some(SetMode::Nx);
                    i += 1;
                }
                arg if option_eq(arg, b"XX") => {
                    if matches!(params.mode, Some(SetMode::Nx)) {
                        return Err(ProtocolError::SyntaxError);
                    }
                    params.mode = Some(SetMode::Xx);
                    i += 1;
                }
                arg if option_eq(arg, b"GET") => {
                    params.get = true;
                    i += 1;
                }
                arg if option_eq(arg, b"EX") => {
                    if !matches!(params.expiration, None | Some(Expiration::Ex(_)))
                        || i + 1 >= items.len()
                    {
                        return Err(ProtocolError::SyntaxError);
                    }
                    expiration_value = Some(&items[i + 1]);
                    params.expiration = Some(Expiration::Ex(0));
                    i += 2;
                }
                arg if option_eq(arg, b"PX") => {
                    if !matches!(params.expiration, None | Some(Expiration::Px(_)))
                        || i + 1 >= items.len()
                    {
                        return Err(ProtocolError::SyntaxError);
                    }
                    expiration_value = Some(&items[i + 1]);
                    params.expiration = Some(Expiration::Px(0));
                    i += 2;
                }
                arg if option_eq(arg, b"EXAT") => {
                    if !matches!(params.expiration, None | Some(Expiration::ExAt(_)))
                        || i + 1 >= items.len()
                    {
                        return Err(ProtocolError::SyntaxError);
                    }
                    expiration_value = Some(&items[i + 1]);
                    params.expiration = Some(Expiration::ExAt(0));
                    i += 2;
                }
                arg if option_eq(arg, b"PXAT") => {
                    if !matches!(params.expiration, None | Some(Expiration::PxAt(_)))
                        || i + 1 >= items.len()
                    {
                        return Err(ProtocolError::SyntaxError);
                    }
                    expiration_value = Some(&items[i + 1]);
                    params.expiration = Some(Expiration::PxAt(0));
                    i += 2;
                }
                arg if option_eq(arg, b"KEEPTTL") => {
                    if !matches!(params.expiration, None | Some(Expiration::KeepTTL)) {
                        return Err(ProtocolError::SyntaxError);
                    }
                    params.expiration = Some(Expiration::KeepTTL);
                    i += 1;
                }
                _ => return Err(ProtocolError::SyntaxError),
            }
        }

        // Redis checks all option conflicts before parsing only the final expiry
        // argument. Earlier same-kind values may be invalid and are overwritten.
        if let Some(value) = expiration_value {
            match params.expiration.as_mut() {
                Some(Expiration::Ex(seconds) | Expiration::ExAt(seconds)) => {
                    *seconds = Self::parse_positive_expiration(value, true)?;
                }
                Some(Expiration::Px(millis) | Expiration::PxAt(millis)) => {
                    *millis = Self::parse_positive_expiration(value, false)?;
                }
                _ => unreachable!("an expiry argument always has an expiry option"),
            }
        }

        Ok(params)
    }
}

/// SET command executor
pub struct SetCommand;

impl RaftCommand for SetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = SetParams::parse(items)?;
        Ok(Operation::Redis(RedisSet(params)))
    }
}

#[async_trait]
impl Command for SetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }
        let params = SetParams::parse(items)?;
        server
            .app
            .write(Operation::Redis(RedisSet(params)), client.db_number)
            .await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetReq {
    pub key: Bytes,
    pub value: Bytes,
    pub ex_time: u64,
}

impl Display for SetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetReq {{ key: {}, value: {}, ex_time: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.ex_time
        )
    }
}

impl ComputeCommand for SetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Set(self.clone())
    }

    fn mutate(
        self,
        _entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let data = match parse_canonical_i64(&self.value) {
            None => ValueObject::String(self.value.clone()),
            Some(v) => ValueObject::Int(v),
        };
        let expire = if self.ex_time == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.ex_time)
        };
        let new_value = MyValue::new(data);
        (
            MochaOperation::Insert {
                value: new_value,
                expire,
            },
            Value::ok(),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let data = match parse_canonical_i64(&self.value) {
            None => ValueObject::String(self.value.clone()),
            Some(v) => ValueObject::Int(v),
        };
        let expire = if self.ex_time == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.ex_time)
        };
        let value = MyValue::new(data);
        (MochaOperation::Insert { value, expire }, Value::ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::do_request;
    use crate::raft::types::entry::request::Request;

    fn bulk(value: &str) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(value.as_bytes())))
    }

    fn parse_options(options: &[&str]) -> Result<SetParams, ProtocolError> {
        let mut items = vec![bulk("SET"), bulk("key"), bulk("value")];
        items.extend(options.iter().map(|value| bulk(value)));
        SetParams::parse(&items)
    }

    #[test]
    fn accepts_repeated_same_kind_flags() {
        for (option, mode) in [("nX", SetMode::Nx), ("xX", SetMode::Xx)] {
            let params =
                parse_options(&[option, "gEt", "kEePtTl", option, "GET", "KEEPTTL", option])
                    .unwrap();
            assert_eq!(params.mode, Some(mode));
            assert!(params.get);
            assert_eq!(params.expiration, Some(Expiration::KeepTTL));
        }
    }

    #[test]
    fn option_names_follow_redis_nul_termination() {
        for name in ["NX", "XX", "GET", "KEEPTTL", "EX", "PX", "EXAT", "PXAT"] {
            let with_nul = format!("{}\0suffix", name.to_ascii_lowercase());
            let mut plain = vec![name];
            let mut suffixed = vec![with_nul.as_str()];
            if ["EX", "PX", "EXAT", "PXAT"].contains(&name) {
                plain.push("2");
                suffixed.push("2");
            }
            assert_eq!(parse_options(&suffixed), parse_options(&plain));
        }
        for invalid in ["", "\0NX", "N\0X", "NXX\0", "GETX\0", "KEEPTTLX\0"] {
            assert_eq!(parse_options(&[invalid]), Err(ProtocolError::SyntaxError));
        }
        assert_eq!(
            parse_options(&["NX\0ignored", "XX\0ignored"]),
            Err(ProtocolError::SyntaxError),
        );
        assert_eq!(parse_options(&["EX\0"]), Err(ProtocolError::SyntaxError));
        assert_eq!(
            parse_options(&["EX\0", "2\0ignored"]),
            Err(ProtocolError::NotAnInteger),
        );
        let mut binary_suffix = vec![0xff; 1024 * 1024];
        binary_suffix[..3].copy_from_slice(b"NX\0");
        let params = SetParams::parse(&[
            bulk("SET"),
            bulk("key"),
            bulk("value"),
            Value::BulkString(Some(binary_suffix.into())),
        ])
        .unwrap();
        assert_eq!(params.mode, Some(SetMode::Nx));
    }

    #[test]
    fn validates_only_the_last_same_kind_expiration() {
        for (option, expected) in [
            ("EX", Expiration::Ex(2)),
            ("PX", Expiration::Px(2)),
            ("EXAT", Expiration::ExAt(2)),
            ("PXAT", Expiration::PxAt(2)),
        ] {
            for replaced in ["1", "0", "-1", "+1", "01", "bad", "9223372036854775808"] {
                let params = parse_options(&[option, replaced, "GET", option, "2"]).unwrap();
                assert_eq!(
                    params.expiration,
                    Some(expected.clone()),
                    "{option} {replaced}"
                );
                assert!(params.get);
            }
            assert_eq!(
                parse_options(&[option, "bad", option, "+2"]),
                Err(ProtocolError::NotAnInteger),
            );
            for invalid in ["0", "-1"] {
                assert_eq!(
                    parse_options(&[option, "2", option, invalid]),
                    Err(SetParams::invalid_expire_time()),
                );
            }
        }
        for option in ["EX", "EXAT"] {
            assert!(parse_options(&[option, "9223372036854776", option, "2"]).is_ok());
            assert_eq!(
                parse_options(&[option, "2", option, "9223372036854776"]),
                Err(SetParams::invalid_expire_time()),
            );
        }
    }

    #[test]
    fn conflicting_options_and_missing_arguments_take_precedence_over_expiry_values() {
        for first in ["EX", "PX", "EXAT", "PXAT"] {
            for second in ["EX", "PX", "EXAT", "PXAT"] {
                if first != second {
                    assert_eq!(
                        parse_options(&[first, "bad", second, "2"]),
                        Err(ProtocolError::SyntaxError),
                        "{first} and {second} conflict",
                    );
                }
            }
            for options in [
                vec![first, "bad", "KEEPTTL"],
                vec!["KEEPTTL", first, "bad"],
                vec![first, "0", "unknown"],
                vec![first, "bad", "NX", "XX"],
                vec![first, "bad", first],
                vec![first],
            ] {
                assert_eq!(parse_options(&options), Err(ProtocolError::SyntaxError));
            }
        }
        for options in [
            vec!["NX", "XX"],
            vec!["XX", "NX"],
            vec!["NX", "NX", "XX"],
            vec!["XX", "XX", "NX"],
        ] {
            assert_eq!(parse_options(&options), Err(ProtocolError::SyntaxError));
        }
    }

    fn apply_replicated_set(caches: &[MyCache; 2], arguments: &[&str], now: u64) -> Vec<u8> {
        let items = arguments
            .iter()
            .map(|value| bulk(value))
            .collect::<Vec<_>>();
        let operation = SetCommand.raft_request(&items).unwrap();
        let log = bincode2::serialize(&Request::new(now, 0, operation)).unwrap();
        let replies = caches.each_ref().map(|cache| {
            let request: Request = bincode2::deserialize(&log).unwrap();
            let (write_clock, db_number) = request.split_u64();
            cache.set_write_clock(write_clock);
            let mut update_type = UpdateType::None;
            let mut update = Update {
                db_number,
                write_clock,
                update_type: &mut update_type,
            };
            do_request(cache, request.operation, &mut update, true).encode()
        });
        assert_eq!(replies[0], replies[1]);
        replies[0].clone()
    }

    fn assert_stored(caches: &[MyCache; 2], expected: &str, expires_at: Option<u64>) {
        for cache in caches {
            let entry = cache.databases[0].mocha.get_entry(&b"key"[..]).unwrap();
            assert!(
                matches!(entry.value.data, ValueObject::String(ref value) if value == expected)
            );
            assert_eq!(entry.expire_at, expires_at);
        }
    }

    #[test]
    fn repeated_options_preserve_values_and_ttls_after_replication() {
        for (option, last_expiry) in [("EX", "2"), ("PX", "2000"), ("EXAT", "6"), ("PXAT", "6000")]
        {
            let caches = [MyCache::new(1).unwrap(), MyCache::new(1).unwrap()];
            assert_eq!(
                apply_replicated_set(
                    &caches,
                    &[
                        "SET",
                        "key",
                        "old",
                        "NX",
                        "NX",
                        option,
                        "bad",
                        option,
                        last_expiry
                    ],
                    4_000,
                ),
                b"+OK\r\n",
            );
            assert_stored(&caches, "old", Some(6_000));

            assert_eq!(
                apply_replicated_set(
                    &caches,
                    &[
                        "SET", "key", "ignored", "NX", "NX", "GET", "GET", "PX", "9000"
                    ],
                    4_100,
                ),
                b"$3\r\nold\r\n",
            );
            assert_stored(&caches, "old", Some(6_000));

            assert_eq!(
                apply_replicated_set(
                    &caches,
                    &[
                        "SET", "key", "new", "XX", "XX", "GET", "GET", "KEEPTTL", "KEEPTTL"
                    ],
                    4_200,
                ),
                b"$3\r\nold\r\n",
            );
            assert_stored(&caches, "new", Some(6_000));

            assert_eq!(
                apply_replicated_set(&caches, &["SET", "key", "ignored", "XX", "XX"], 7_000),
                b"$-1\r\n",
            );
            for cache in &caches {
                assert!(cache.databases[0].mocha.get_entry(&b"key"[..]).is_none());
            }
            assert_eq!(
                apply_replicated_set(
                    &caches,
                    &["SET", "key", "persistent", "NX", "NX", "KEEPTTL", "KEEPTTL"],
                    7_100,
                ),
                b"+OK\r\n",
            );
            assert_stored(&caches, "persistent", None);
        }
    }

    #[test]
    fn nul_terminated_options_preserve_binary_values_and_ttl_after_replication() {
        let caches = [MyCache::new(1).unwrap(), MyCache::new(1).unwrap()];
        assert_eq!(
            apply_replicated_set(
                &caches,
                &[
                    "SET",
                    "key",
                    "old\0tail",
                    "NX\0suffix",
                    "PX\0suffix",
                    "2000"
                ],
                4_000,
            ),
            b"+OK\r\n",
        );
        assert_stored(&caches, "old\0tail", Some(6_000));
        assert_eq!(
            apply_replicated_set(
                &caches,
                &[
                    "SET",
                    "key",
                    "new\0tail",
                    "XX\0suffix",
                    "GET\0suffix",
                    "KEEPTTL\0suffix"
                ],
                4_100,
            ),
            b"$8\r\nold\0tail\r\n",
        );
        assert_stored(&caches, "new\0tail", Some(6_000));
    }

    #[test]
    fn final_expiration_overflow_after_replication_does_not_modify_the_key() {
        let caches = [MyCache::new(1).unwrap(), MyCache::new(1).unwrap()];
        apply_replicated_set(&caches, &["SET", "key", "old"], 4_000);
        for (option, last_expiry) in [("EX", "9223372036854775"), ("PX", "9223372036854775807")] {
            assert_eq!(
                apply_replicated_set(
                    &caches,
                    &["SET", "key", "new", option, "1", option, last_expiry],
                    4_000,
                ),
                b"-ERR invalid expire time in 'set' command\r\n",
            );
            assert_stored(&caches, "old", None);
        }
    }

    #[test]
    fn rejects_second_expiration_when_millisecond_conversion_overflows() {
        let items = [
            bulk("SET"),
            bulk("key"),
            bulk("value"),
            bulk("EX"),
            bulk("9223372036854776"),
        ];
        assert_eq!(
            SetParams::parse(&items),
            Err(ProtocolError::response(
                "ERR invalid expire time in 'set' command"
            ))
        );
    }
}
