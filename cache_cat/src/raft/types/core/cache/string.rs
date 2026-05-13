use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::string::get::GetParams;
use crate::protocol::string::mget::MgetParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::response_value::Value::Error;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{AppendReq, BaseOperation, IncrReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::parse_i64;
use std::sync::Arc;

impl ComputeCommand for IncrReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Incr(self.clone())
    }

    fn mutate(self, value: &mut MyValue) -> (bool, Value) {
        match &mut value.data {
            ValueObject::Int(n) => {
                *n += self.value;
                (true, Value::Integer(*n))
            }

            ValueObject::String(s) => {
                if let Some(v) = parse_i64(s) {
                    let new_val = v + self.value;
                    value.data = ValueObject::Int(new_val);
                    (true, Value::Integer(new_val))
                } else {
                    (false, Value::Error("Value is not an integer".to_string()))
                }
            }

            _ => (
                false,
                Value::Error("Key exists but is not an Integer".to_string()),
            ),
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let v = self.value;
        (ValueObject::Int(v), Value::Integer(v))
    }
}

impl ComputeCommand for AppendReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Append(self.clone())
    }

    fn mutate(self, value: &mut MyValue) -> (bool, Value) {
        match &mut value.data {
            ValueObject::String(data_arc) => {
                let buf = Arc::make_mut(data_arc);
                buf.extend_from_slice(&self.value);

                let len = buf.len() as i64;
                (true, Value::Integer(len))
            }

            _ => (
                false,
                Value::Error("Key exists but is not a String".to_string()),
            ),
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let len = self.value.len() as i64;
        (ValueObject::String(self.value), Value::Integer(len))
    }
}

impl MyCache {
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
            UpdateType::Snapshot(queue, write_clock) => {
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
                        write_clock: *write_clock,
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
