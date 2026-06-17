use crate::error::ProtocolError;
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::NO_EXPIRATION;
use crate::protocol::string::get::GetParams;
use crate::protocol::string::len::StrLenParams;
use crate::protocol::string::mget::MgetParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::{Expiration, SetMode, SetParams};
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::{MultiReadCommand, ReadCommand};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{AppendReq, BaseOperation, IncrReq, SetReq};
use crate::utils::parse_i64;
use bytes::{Bytes, BytesMut};

impl ComputeCommand for SetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Set(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let new_version = entry.value.version + 1;
        let data = match parse_i64(&self.value) {
            None => ValueObject::String(self.value.clone()),
            Some(v) => ValueObject::Int(v),
        };
        let expire = if self.ex_time == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.ex_time)
        };
        let new_value = MyValue {
            version: new_version,
            data,
        };
        (
            MochaOperation::Insert {
                value: new_value,
                expire,
            },
            Value::ok(),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let data = match parse_i64(&self.value) {
            None => ValueObject::String(self.value.clone()),
            Some(v) => ValueObject::Int(v),
        };
        let expire = if self.ex_time == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.ex_time)
        };
        let value = MyValue { version: 1, data };
        (MochaOperation::Insert { value, expire }, Value::ok())
    }
}

impl ComputeCommand for IncrReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Incr(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let (result, value) = match &entry.value.data {
            ValueObject::Int(n) => {
                let num = n + self.value;
                (ValueObject::Int(num), Value::Integer(num))
            }
            ValueObject::String(s) => {
                if let Some(v) = parse_i64(&s) {
                    let new_val = v + self.value;
                    (ValueObject::Int(new_val), Value::Integer(new_val))
                } else {
                    return (
                        MochaOperation::Abort,
                        Value::Error(Bytes::from_static(b"Value is not an integer")),
                    );
                }
            }
            _ => {
                return (
                    MochaOperation::Abort,
                    Value::Error(Bytes::from_static(b"Key exists but is not an Integer")),
                );
            }
        };
        (
            MochaOperation::Insert {
                value: MyValue::new(result),
                expire: entry.get_expire_policy(),
            },
            value,
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let v = self.value;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Int(v)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(v),
        )
    }
}

impl ComputeCommand for AppendReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Append(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::String(data_arc) => {
                // 构造新的字符串：原内容 + 追加内容
                let mut new_buf: BytesMut = data_arc.clone().into();
                new_buf.extend_from_slice(&self.value);
                let len = new_buf.len() as i64;
                let new_value = MyValue::new(ValueObject::String(new_buf.freeze()));
                (
                    MochaOperation::Insert {
                        value: new_value,
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(len),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(Bytes::from_static(b"Key exists but is not a String")),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let len = self.value.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(self.value)),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}

impl ReadCommand for GetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Int(int_value) => {
                    Value::BulkString(Some(int_value.to_string().into()))
                }
                ValueObject::String(str_value) => Value::BulkString(Some(str_value)),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}
impl ReadCommand for StrLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        let len = match value {
            None => 0,
            Some(v) => match v.data {
                ValueObject::String(ref bytes) => bytes.len(),
                ValueObject::Int(ref i) => i.to_string().len(),
                _ => return ProtocolError::WrongType.into(),
            },
        };
        Value::Integer(len as i64)
    }
}

impl MultiReadCommand for MgetParams {
    fn keys(&self) -> &Vec<Bytes> {
        &self.keys
    }

    fn execute(&self, values: Vec<Option<MyValue>>) -> Value {
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            results.push(match value {
                None => Value::BulkString(None),

                Some(v) => match v.data {
                    ValueObject::Int(int_value) => {
                        Value::BulkString(Some(int_value.to_string().into()))
                    }

                    ValueObject::String(str_value) => Value::BulkString(Some(str_value)),

                    _ => ProtocolError::WrongType.into(),
                },
            });
        }

        Value::Array(Some(results))
    }
}
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
        // 最新的写逻辑时间
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
            Some(SetMode::Nx) => {
                // NX: Only set if key does not exist
                if key_exists {
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

    pub fn m_get(&self, param: MgetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_multi_read(param, db_number, read_clock)
    }

    pub fn get(&self, param: GetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn str_len(&self, param: StrLenParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn set(&self, param: SetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn incr(&self, param: IncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    //如果不是string就报错，如果是string就append，如果没有值就创建一个
    pub fn append(&self, param: AppendReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
