use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hmget::HMGetParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, HDelReq, HIncrReq, HSetReq};
use crate::utils::parse_i64;
use moka::ops::compute::Op;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

impl ComputeCommand for HSetReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HSet(self.clone())
    }

    fn mutate(self, mut data: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        if let ValueObject::Hash(map_arc) = &data.data {
            let mut count = 0;
            {
                let mut map = map_arc.lock();

                for (k, v) in &self.elements {
                    let value = parse_i64(v)
                        .map(HashValue::Int)
                        .unwrap_or_else(|| HashValue::Str(v.clone()));

                    if map.insert(k.clone(), value).is_none() {
                        count += 1;
                    }
                }
            } // map 在这里 drop
            (Op::Put(data), Value::Integer(count))
        } else {
            (
                Op::Nop,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            )
        }
    }

    fn init(self) -> (Op<MyValue>, Value) {
        let mut map = HashMap::new();
        let len = self.elements.len();
        for (k, v) in self.elements {
            if let Some(int) = parse_i64(&v) {
                map.insert(k.clone(), HashValue::Int(int));
            } else {
                map.insert(k.clone(), HashValue::Str(v.clone()));
            }
        }
        (
            Op::Put(MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map))))),
            Value::Integer(len as i64),
        )
    }
}
impl ComputeCommand for HIncrReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HIncr(self.clone())
    }

    fn mutate(self, mut data: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        match &mut data.data {
            ValueObject::Hash(hash) => {
                let result = {
                    let mut hash = hash.lock();
                    let hash_value = hash.get(&self.field);
                    match hash_value {
                        Some(HashValue::Int(int)) => {
                            let new_int = *int + self.value;
                            hash.insert(self.field.clone(), HashValue::Int(new_int));
                            Value::Integer(new_int)
                        }
                        Some(HashValue::Str(_)) => {
                            return (
                                Op::Nop,
                                Value::Error("ERR hash value is not an integer".into()),
                            );
                        }
                        None => {
                            hash.insert(self.field.clone(), HashValue::Int(self.value));
                            Value::Integer(self.value)
                        }
                    }
                };
                (Op::Put(data), result)
            }
            _ => (
                Op::Nop,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (Op<MyValue>, Value) {
        let mut map = HashMap::new();
        map.insert(self.field, HashValue::Int(self.value));
        (
            Op::Put(MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map))))),
            Value::Integer(self.value),
        )
    }
}

impl ComputeCommand for HDelReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HDel(self.clone())
    }

    fn mutate(self, mut data: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        match &mut data.data {
            ValueObject::Hash(hash) => {
                let deleted_count = {
                    let mut hash = hash.lock();
                    let mut count = 0;
                    for field in &self.fields {
                        if hash.remove(field).is_some() {
                            count += 1;
                        }
                    }
                    count
                };
                if deleted_count == 0 {
                    return (Op::Nop, Value::Integer(deleted_count));
                }
                (Op::Put(data), Value::Integer(deleted_count))
            }
            _ => (
                Op::Nop,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (Op<MyValue>, Value) {
        //key本身不存在也返回9
        (Op::Remove, Value::Integer(0))
    }
}

impl MyCache {
    pub fn h_m_get(&self, param: HMGetParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&param.key) {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let results: Vec<Value> = param
                        .fields
                        .iter()
                        .map(|field| match guard.get(field) {
                            None => Value::BulkString(None),
                            Some(value) => match value {
                                HashValue::Str(str) => {
                                    Value::BulkString(Some(str.as_ref().clone()))
                                }
                                HashValue::Int(int) => {
                                    Value::BulkString(Some(int.to_string().as_bytes().to_vec()))
                                }
                            },
                        })
                        .collect();
                    Value::Array(Some(results))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
    pub fn h_get(&self, param: HGetParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&param.key) {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&param.field);
                    match option {
                        None => Value::BulkString(None),
                        Some(value) => match value {
                            HashValue::Str(str) => Value::BulkString(Some(str.as_ref().clone())),
                            HashValue::Int(int) => {
                                Value::BulkString(Some(int.to_string().as_bytes().to_vec()))
                            }
                        },
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
    pub fn h_del(&self, param: HDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn h_set(&self, param: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_incr(&self, param: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
