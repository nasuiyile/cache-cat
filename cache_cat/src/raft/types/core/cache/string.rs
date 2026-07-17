use crate::error::ProtocolError;
use crate::mocha::EntrySnapshot;
use crate::protocol::NO_EXPIRATION;
use crate::protocol::string::append::AppendReq;
use crate::protocol::string::decr::DecrReq;
use crate::protocol::string::decrby::DecrByReq;
use crate::protocol::string::getset::GetSetParams;
use crate::protocol::string::incr::IncrReq;
use crate::protocol::string::incrby::IncrByReq;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::psetex::PSetExParams;
use crate::protocol::string::set::{Expiration, SetMode, SetParams, SetReq};
use crate::protocol::string::setex::SetExParams;
use crate::protocol::string::setnx::SetNxParams;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use bytes::Bytes;

impl MyCache {
    pub fn redis_mset(&self, params: MsetParams, update: &mut Update<'_>, external: bool) -> Value {
        if external {
            let _exclusive_lock = self.read_lock.write();
        }
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
                Expiration::Ex(seconds) => now + seconds * 1000,
                Expiration::Px(millis) => now + millis,
                Expiration::ExAt(timestamp) => timestamp * 1000,
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
        enum ExistingKey {
            None,      // Key doesn't exist
            Data,      // Key exists and is a valid string
            OtherType, // Key exists but is not a string (Hash, etc.)
        }

        let mut existing_key = ExistingKey::None;

        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.mocha.get_entry(&params.key) {
            None => { /* remains None */ }
            Some(value) => {
                existing_key = match value.value.data {
                    ValueObject::Int(_) => ExistingKey::Data,
                    ValueObject::String(_) => ExistingKey::Data,
                    _ => ExistingKey::OtherType,
                };
            }
        }

        if matches!(existing_key, ExistingKey::Data | ExistingKey::OtherType) {
            // Just return nil (nil bulk string)
            Value::BulkString(None)
        } else {
            let set = SetReq {
                key: params.key,
                value: params.value,
                ex_time: NO_EXPIRATION,
            };

            self.set(set, update);

            Value::ok()
        }
    }

    pub fn redis_setex(&self, params: SetExParams, update: &mut Update<'_>) -> Value {
        // The latest write logic time
        let now = update.write_clock;

        let expires_at = now + params.expiration * 1000;

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

        let expires_at = now + params.expiration;

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
}
