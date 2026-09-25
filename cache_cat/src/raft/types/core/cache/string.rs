use crate::error::ProtocolError;
use crate::protocol::string::append::AppendReq;
use crate::protocol::string::decr::DecrReq;
use crate::protocol::string::decrby::DecrByReq;
use crate::protocol::string::fadd::PfAddReq;
use crate::protocol::string::getset::GetSetParams;
use crate::protocol::string::incr::IncrReq;
use crate::protocol::string::incrby::IncrByReq;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::psetex::PSetExParams;
use crate::protocol::string::set::{Expiration, SetMode, SetParams, SetReq};
use crate::protocol::string::setex::SetExParams;
use crate::protocol::string::setnx::SetNxParams;
use crate::protocol::NO_EXPIRATION;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use bytes::Bytes;

impl MyCache {
    pub fn redis_mset(&self, params: MsetParams, update: &mut Update<'_>, external: bool) -> Value {
        let _exclusive_lock = if external {
            Some(self.read_lock.write())
        } else {
            None
        };
        for pair in params.pairs {
            let set = SetReq {
                key: pair.0,
                value: pair.1,
                ex_time: 0,
            };
            self.set(set, update);
        }
        Value::ok()
    }

    pub fn redis_set(&self, params: SetParams, update: &mut Update<'_>) -> Value {
        // The latest write logic time
        let now = update.write_clock;

        enum ExistingKey {
            None,        // Key doesn't exist
            Data(Bytes), // Key exists and is a valid string
            OtherType,   // Key exists but is not a string (Hash, etc.)
        }
        let mut existing_key = ExistingKey::None;

        // Calculate expiration timestamp in milliseconds (0 means no expiration)
        let expires_at = match params.expiration {
            Some(Expiration::KeepTTL) => {
                let cache = match self.get_cache(update.db_number) {
                    Err(err) => return err,
                    Ok(cache) => cache,
                };
                // Read existing value to get its expiration time
                match cache.mocha.get_entry(&params.key) {
                    None => NO_EXPIRATION,
                    Some(value) => {
                        let ttl_ms = value.expire_at.unwrap_or(0);
                        existing_key = match value.value.data {
                            ValueObject::Int(v) => ExistingKey::Data(v.to_string().into()),
                            ValueObject::String(v) => ExistingKey::Data(v),
                            _ => ExistingKey::OtherType,
                        };
                        ttl_ms
                    }
                }
            }
            Some(exp) => match exp {
                Expiration::Ex(seconds) => now.saturating_add(seconds.saturating_mul(1000)),
                Expiration::Px(millis) => now.saturating_add(millis),
                Expiration::ExAt(timestamp) => timestamp.saturating_mul(1000),
                Expiration::PxAt(timestamp) => timestamp,
                Expiration::KeepTTL => unreachable!(), // Handled above
            },
            None => NO_EXPIRATION, // No expiration
        };

        if matches!(existing_key, ExistingKey::None) && (params.mode.is_some() || params.get) {
            let cache = match self.get_cache(update.db_number) {
                Err(err) => return err,
                Ok(cache) => cache,
            };
            match cache.mocha.get_entry(&params.key) {
                None => { /* remains None */ }
                Some(value) => {
                    existing_key = match value.value.data {
                        ValueObject::Int(v) => ExistingKey::Data(v.to_string().into()),
                        ValueObject::String(v) => ExistingKey::Data(v),
                        _ => ExistingKey::OtherType,
                    };
                }
            }
        }

        // Redis runs the GET half first (setGenericCommand -> getGenericCommand):
        // a key holding a non-string value fails with WRONGTYPE before NX/XX is
        // evaluated, and nothing is written.
        if params.get && matches!(existing_key, ExistingKey::OtherType) {
            return ProtocolError::WrongType.into();
        }

        let key_exists = matches!(existing_key, ExistingKey::Data(_) | ExistingKey::OtherType);

        // Apply NX/XX mode logic
        match params.mode {
            // NX: Only set if key does not exist
            Some(SetMode::Nx) if key_exists => {
                // Key exists, do not set
                return if params.get {
                    // GET with NX: return current value if it's a string, otherwise nil
                    match existing_key {
                        ExistingKey::Data(v) => Value::BulkString(Some(v)),
                        _ => Value::BulkString(None), // Other type, return nil
                    }
                } else {
                    // Just return nil (nil bulk string)
                    Value::BulkString(None)
                };
            }

            Some(SetMode::Xx) if !key_exists => {
                // Key does not exist, do not set
                return if params.get {
                    // GET with XX: return nil since key doesn't exist
                    Value::BulkString(None)
                } else {
                    Value::BulkString(None)
                };
            }

            None => {
                // No mode restriction, always set
            }

            _ => {}
        }

        let set = SetReq {
            key: params.key,
            value: params.value,
            ex_time: expires_at,
        };
        self.set(set, update);
        if params.get {
            // Store the old value for GET option before we overwrite
            match existing_key {
                ExistingKey::Data(v) => Value::BulkString(Some(v)),
                _ => Value::BulkString(None), // Other type, return nil
            }
        } else {
            Value::ok()
        }
    }

    pub fn redis_setnx(&self, params: SetNxParams, update: &mut Update<'_>) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };

        // SETNX replies with an integer: 0 when the key already holds a value
        // of any type (nothing is written), 1 when the key was set.
        if cache.mocha.get_entry(&params.key).is_some() {
            return Value::Integer(0);
        }

        let set = SetReq {
            key: params.key,
            value: params.value,
            ex_time: NO_EXPIRATION,
        };
        self.set(set, update);

        Value::Integer(1)
    }

    pub fn redis_setex(&self, params: SetExParams, update: &mut Update<'_>) -> Value {
        // The latest write logic time
        let now = update.write_clock;

        // SetExParams stores the parsed duration in milliseconds, matching
        // PSETEX and the logical clock units used by the cache.
        let expires_at = now.saturating_add(params.expiration);

        let set = SetReq {
            key: params.key,
            value: params.value,
            ex_time: expires_at,
        };

        self.set(set, update);

        Value::ok()
    }

    pub fn redis_psetex(&self, params: PSetExParams, update: &mut Update<'_>) -> Value {
        // The latest write logic time
        let now = update.write_clock;

        let expires_at = now.saturating_add(params.expiration);

        let set = SetReq {
            key: params.key,
            value: params.value,
            ex_time: expires_at,
        };

        self.set(set, update);

        Value::ok()
    }
    pub fn redis_get_set(&self, params: GetSetParams, update: &mut Update<'_>) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let res = match cache.mocha.get_entry(&params.key) {
            None => Value::BulkString(None),
            Some(v) => match v.value.data {
                ValueObject::Int(v) => Value::BulkString(Some(v.to_string().into())),
                ValueObject::String(v) => Value::BulkString(Some(v)),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        let set = SetReq {
            key: params.key,
            value: params.value,
            ex_time: 0,
        };
        self.set(set, update);
        res
    }

    pub fn set(&self, param: SetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn incr(&self, param: IncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn decr(&self, param: DecrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn incr_by(&self, param: IncrByReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    // If it's not a string, report an error;
    // if it's a string, append; if there's no value, create one
    pub fn append(&self, param: AppendReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn decr_by(&self, param: DecrByReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn p_f_add(&self, param: PfAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::hash::hget::HGetParams;
    use crate::protocol::hash::hset::HSetReq;
    use crate::protocol::string::get::GetParams;
    use crate::raft::types::core::mocha::core::UpdateType;
    use crate::raft::types::core::mocha::request_handler::do_request;
    use crate::raft::types::entry::base_operation::BaseOperation;
    use crate::raft::types::entry::read_operation::ReadOperation;
    use crate::raft::types::entry::request::{Operation, RedisOperation};

    const WRONG_TYPE: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    const NIL: &[u8] = b"$-1\r\n";

    fn apply(cache: &MyCache, operation: Operation) -> Vec<u8> {
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: 1,
            update_type: &mut update_type,
        };
        do_request(cache, operation, &mut update, true).encode()
    }

    fn set(cache: &MyCache, params: SetParams) -> Vec<u8> {
        apply(cache, Operation::Redis(RedisOperation::RedisSet(params)))
    }

    fn set_get(key: &'static str, value: &'static str, mode: Option<SetMode>) -> SetParams {
        SetParams {
            mode,
            get: true,
            ..SetParams::new(key, value)
        }
    }

    fn setnx(cache: &MyCache, key: &'static str, value: &'static str) -> Vec<u8> {
        let params = SetNxParams {
            key: key.into(),
            value: value.into(),
        };
        apply(cache, Operation::Redis(RedisOperation::RedisSetNx(params)))
    }

    fn get(cache: &MyCache, key: &'static str) -> Vec<u8> {
        let params = GetParams { key: key.into() };
        apply(cache, Operation::Read(ReadOperation::Get(params)))
    }

    fn hset(cache: &MyCache, key: &'static str) {
        let req = HSetReq {
            key: key.into(),
            elements: vec![("field".into(), "value".into())],
        };
        assert_eq!(
            apply(cache, Operation::Base(BaseOperation::HSet(req))),
            b":1\r\n"
        );
    }

    fn bulk(value: &str) -> Vec<u8> {
        format!("${}\r\n{}\r\n", value.len(), value).into_bytes()
    }

    /// INCR, DECR, INCRBY 5 and DECRBY 5 against `key`.
    fn incr_family(key: &'static str) -> [BaseOperation; 4] {
        let key = Bytes::from_static(key.as_bytes());
        [
            BaseOperation::Incr(IncrReq { key: key.clone() }),
            BaseOperation::Decr(DecrReq { key: key.clone() }),
            BaseOperation::IncrBy(IncrByReq {
                key: key.clone(),
                increment: 5,
            }),
            BaseOperation::DecrBy(DecrByReq { key, decrement: 5 }),
        ]
    }

    fn hget(cache: &MyCache, key: &'static str) -> Vec<u8> {
        let params = HGetParams {
            key: key.into(),
            field: "field".into(),
        };
        apply(cache, Operation::Read(ReadOperation::HGet(params)))
    }

    #[test]
    fn set_keeps_non_canonical_integer_strings_verbatim() {
        let cache = MyCache::new(1).expect("cache");

        for value in ["01", "+1", "-0", " 1", "1 ", "007", "9223372036854775808"] {
            assert_eq!(set(&cache, SetParams::new("k", value)), b"+OK\r\n");
            let expected = format!("${}\r\n{}\r\n", value.len(), value);
            assert_eq!(get(&cache, "k"), expected.as_bytes(), "SET k {value:?}");
        }

        // Canonical integers still use the integer encoding and read back unchanged.
        for value in [
            "0",
            "1",
            "-1",
            "9223372036854775807",
            "-9223372036854775808",
        ] {
            assert_eq!(set(&cache, SetParams::new("k", value)), b"+OK\r\n");
            let entry = cache.databases[0]
                .mocha
                .get_entry(&b"k"[..])
                .expect("entry");
            assert!(matches!(entry.value.data, ValueObject::Int(_)), "{value}");
            let expected = format!("${}\r\n{}\r\n", value.len(), value);
            assert_eq!(get(&cache, "k"), expected.as_bytes());
        }
    }

    #[test]
    fn set_get_returns_the_previous_string_verbatim() {
        let cache = MyCache::new(1).expect("cache");

        assert_eq!(set(&cache, set_get("k", "01", None)), NIL);
        assert_eq!(set(&cache, set_get("k", "v2", None)), b"$2\r\n01\r\n");
        assert_eq!(get(&cache, "k"), b"$2\r\nv2\r\n");
    }

    #[test]
    fn set_get_on_a_hash_is_wrongtype_and_does_not_overwrite() {
        let cache = MyCache::new(1).expect("cache");
        hset(&cache, "h");

        for mode in [None, Some(SetMode::Nx), Some(SetMode::Xx)] {
            assert_eq!(
                set(&cache, set_get("h", "v", mode.clone())),
                WRONG_TYPE,
                "{mode:?}"
            );
            assert_eq!(hget(&cache, "h"), b"$5\r\nvalue\r\n", "{mode:?}");
        }

        let keep_ttl = SetParams {
            expiration: Some(Expiration::KeepTTL),
            ..set_get("h", "v", None)
        };
        assert_eq!(set(&cache, keep_ttl), WRONG_TYPE);
        assert_eq!(hget(&cache, "h"), b"$5\r\nvalue\r\n");

        // Without GET, SET still replaces a key of any type, as Redis does.
        assert_eq!(set(&cache, SetParams::new("h", "v")), b"+OK\r\n");
        assert_eq!(get(&cache, "h"), b"$1\r\nv\r\n");
    }

    #[test]
    fn setnx_replies_with_an_integer() {
        let cache = MyCache::new(1).expect("cache");

        assert_eq!(setnx(&cache, "k", "01"), b":1\r\n");
        assert_eq!(setnx(&cache, "k", "other"), b":0\r\n");
        assert_eq!(get(&cache, "k"), b"$2\r\n01\r\n");

        // An existing key of another type also blocks SETNX and is left untouched.
        hset(&cache, "h");
        assert_eq!(setnx(&cache, "h", "v"), b":0\r\n");
        assert_eq!(hget(&cache, "h"), b"$5\r\nvalue\r\n");
    }

    #[test]
    fn incr_family_requires_a_canonical_stored_integer() {
        const NOT_AN_INTEGER: &[u8] = b"-ERR value is not an integer or out of range\r\n";
        let cache = MyCache::new(1).expect("cache");

        // Redis string2ll: no whitespace, no '+', no leading zeros, no "-0".
        for value in [
            "01",
            "+1",
            "-0",
            " 1",
            "1 ",
            "",
            "1.0",
            "9223372036854775808",
        ] {
            for operation in incr_family("k") {
                assert_eq!(set(&cache, SetParams::new("k", value)), b"+OK\r\n");
                assert_eq!(
                    apply(&cache, Operation::Base(operation)),
                    NOT_AN_INTEGER,
                    "{value:?}"
                );
                assert_eq!(
                    get(&cache, "k"),
                    bulk(value),
                    "value must be left untouched"
                );
            }
        }

        assert_eq!(set(&cache, SetParams::new("k", "10")), b"+OK\r\n");
        let replies = incr_family("k").map(|op| apply(&cache, Operation::Base(op)));
        assert_eq!(
            replies,
            [&b":11\r\n"[..], b":10\r\n", b":15\r\n", b":10\r\n"]
        );

        // A raw string that happens to be canonical is still a number: "1" + "0".
        assert_eq!(set(&cache, SetParams::new("k", "1")), b"+OK\r\n");
        let append = AppendReq {
            key: "k".into(),
            value: "0".into(),
        };
        assert_eq!(
            apply(&cache, Operation::Base(BaseOperation::Append(append))),
            b":2\r\n"
        );
        let incr = BaseOperation::Incr(IncrReq { key: "k".into() });
        assert_eq!(apply(&cache, Operation::Base(incr)), b":11\r\n");
    }
}
