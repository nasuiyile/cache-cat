use crate::error::ProtocolError;
use crate::protocol::NO_EXPIRATION;
use crate::protocol::string::get::GetParams;
use crate::protocol::string::mget::MgetParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::{Expiration, SetMode, SetParams};
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{AppendReq, BaseOperation, IncrReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::parse_i64;
use moka::ops::compute::Op;
use std::sync::Arc;

impl ComputeCommand for IncrReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Incr(self.clone())
    }

    fn mutate(self, mut value: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        let result = match &mut value.data {
            ValueObject::Int(n) => {
                *n += self.value;
                Value::Integer(*n)
            }
            ValueObject::String(s) => {
                if let Some(v) = parse_i64(s) {
                    let new_val = v + self.value;
                    value.data = ValueObject::Int(new_val);
                    Value::Integer(new_val)
                } else {
                    return (Op::Nop, Value::Error("Value is not an integer".to_string()));
                }
            }
            _ => {
                return (
                    Op::Nop,
                    Value::Error("Key exists but is not an Integer".to_string()),
                );
            }
        };
        (Op::Put(value), result)
    }

    fn init(self) -> (Op<MyValue>, Value) {
        let v = self.value;
        (
            Op::Put(MyValue::new(ValueObject::Int(v))),
            Value::Integer(v),
        )
    }
}

impl ComputeCommand for AppendReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Append(self.clone())
    }

    fn mutate(self, mut data: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        match &mut data.data {
            ValueObject::String(data_arc) => {
                let len = {
                    let buf = Arc::make_mut(data_arc);
                    buf.extend_from_slice(&self.value);
                    buf.len() as i64
                };
                (Op::Put(data), Value::Integer(len))
            }
            _ => (
                Op::Nop,
                Value::Error("Key exists but is not a String".to_string()),
            ),
        }
    }

    fn init(self) -> (Op<MyValue>, Value) {
        let len = self.value.len() as i64;
        (
            Op::Put(MyValue::new(ValueObject::String(self.value))),
            Value::Integer(len),
        )
    }
}

impl MyCache {
    pub fn redis_mset(&self, params: MsetParams, update: &mut Update<'_>, external: bool) -> Value {
        if external {
            let _exclusive_lock = self.read_lock.write();
        }
        for pair in params.pairs {
            let set = SetReq {
                key: Arc::from(pair.0),
                value: Arc::from(pair.1),
                ex_time: 0,
            };
            self.set(set, update);
        }
        Value::ok()
    }

    pub fn redis_set(&self, params: SetParams, update: &mut Update<'_>) -> Value {
        // 最新的写逻辑时间
        let now = update.write_clock;

        enum ExistingKey {
            None,               // Key doesn't exist
            Data(Arc<Vec<u8>>), // Key exists and is a valid string
            OtherType,          // Key exists but is not a string (Hash, etc.)
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
                match cache.get(&params.key) {
                    None => NO_EXPIRATION,
                    Some(value) => {
                        let ttl_ms = value.expires_at;
                        existing_key = match value.data {
                            ValueObject::Int(v) => {
                                ExistingKey::Data(Arc::from(v.to_string().into_bytes()))
                            }
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
        let key_exists = matches!(existing_key, ExistingKey::Data(_) | ExistingKey::OtherType);

        // Apply NX/XX mode logic
        match params.mode {
            Some(SetMode::Nx) => {
                // NX: Only set if key does not exist
                if key_exists {
                    // Key exists, do not set
                    return if params.get {
                        // GET with NX: return current value if it's a string, otherwise nil
                        match existing_key {
                            ExistingKey::Data(v) => Value::BulkString(Some(v.as_ref().clone())),
                            _ => Value::BulkString(None), // Other type, return nil
                        }
                    } else {
                        // Just return nil (nil bulk string)
                        Value::BulkString(None)
                    };
                }
            }
            Some(SetMode::Xx) => {
                // XX: Only set if key exists
                if !key_exists {
                    // Key does not exist, do not set
                    return if params.get {
                        // GET with XX: return nil since key doesn't exist
                        Value::BulkString(None)
                    } else {
                        Value::BulkString(None)
                    };
                }
            }
            None => {
                // No mode restriction, always set
            }
        }
        let set = SetReq {
            key: Arc::from(params.key),
            value: Arc::from(params.value),
            ex_time: expires_at,
        };
        self.set(set, update);
        if params.get {
            // Store the old value for GET option before we overwrite
            match existing_key {
                ExistingKey::Data(v) => Value::BulkString(Some(v.as_ref().clone())),
                _ => Value::BulkString(None), // Other type, return nil
            }
        } else {
            Value::ok()
        }
    }

    pub fn m_get(&self, param: MgetParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut results = Vec::with_capacity(param.keys.len());
        for key in param.keys {
            results.push(match cache.get(&key) {
                None => Value::BulkString(None),
                Some(v) => match v.data {
                    ValueObject::Int(int_value) => {
                        Value::BulkString(Some(int_value.to_string().into_bytes()))
                    }
                    ValueObject::String(str_value) => {
                        Value::BulkString(Some(str_value.as_ref().clone()))
                    }
                    _ => ProtocolError::WrongType.into(),
                },
            });
        }
        Value::Array(Some(results))
    }

    pub fn get(&self, param: GetParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&param.key) {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Int(int_value) => {
                    Value::BulkString(Some(int_value.to_string().into_bytes()))
                }
                ValueObject::String(str_value) => {
                    Value::BulkString(Some(str_value.as_ref().clone()))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }

    pub fn set(&self, set_req: SetReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut value = match parse_i64(&set_req.value) {
            None => MyValue {
                data: ValueObject::String(set_req.value.clone()),
                expires_at: set_req.ex_time,
                version: 1,
            },
            Some(v) => MyValue {
                data: ValueObject::Int(v),
                expires_at: set_req.ex_time,
                version: 1,
            },
        };
        match update.update_type {
            UpdateType::None => {
                cache.insert(set_req.key, value);
                Value::ok()
            }
            UpdateType::Snapshot(queue) => {
                let key = set_req.key.clone();
                cache.entry(key).and_upsert_with(|old_entry| {
                    value.version = if let Some(entry) = old_entry {
                        entry.into_value().version + 1
                    } else {
                        1
                    };
                    queue.push(AtomicRequest {
                        version: value.version,
                        request: BaseOperation::Set(set_req),
                        write_clock: update.write_clock,
                    });
                    value
                });
                Value::ok()
            }
            UpdateType::CAS(cas_version) => {
                let key = set_req.key.clone();
                cache.entry(key).and_upsert_with(|maybe_entry| {
                    if let Some(entry) = maybe_entry {
                        let current_val = entry.value();
                        // 核心逻辑：只有传入的 version 与缓存中的 version 相同时才允许更新
                        if *cas_version - 1 == current_val.version {
                            value.version += 1;
                            value
                        } else {
                            // 版本不匹配，直接返回旧值（即不更新）
                            current_val.clone()
                        }
                    } else {
                        let new_data = ValueObject::String(set_req.value);
                        let ttl = set_req.ex_time;
                        MyValue {
                            data: new_data,
                            expires_at: ttl,
                            version: 1, // 初始版本
                        }
                    }
                });
                Value::ok()
            }
        }
    }

    pub fn incr(&self, incr_req: IncrReq, update: &mut Update) -> Value {
        self.execute_compute(incr_req, update)
    }
    //如果不是string就报错，如果是string就append，如果没有值就创建一个
    pub fn append(&self, incr_req: AppendReq, update: &mut Update) -> Value {
        self.execute_compute(incr_req, update)
    }
}
