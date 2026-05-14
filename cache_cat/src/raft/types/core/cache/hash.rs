use crate::error::ProtocolError;
use crate::protocol::hash::hget::HGetParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, HIncrReq, HSetReq};
use crate::utils::parse_i64;
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

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        if let ValueObject::Hash(map_arc) = &data.data {
            let mut count = 0;
            let mut map = map_arc.lock();
            for (k, v) in &self.elements {
                if let Some(int) = parse_i64(v) {
                    if map.insert(k.clone(), HashValue::Int(int)).is_none() {
                        count += 1;
                    }
                } else {
                    if map.insert(k.clone(), HashValue::Str(v.clone())).is_none() {
                        count += 1;
                    }
                }
            }
            // 返回 true 表示数据已变动，需要更新缓存
            (true, Value::Integer(count))
        } else {
            (
                false,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            )
        }
    }

    fn init(self) -> (ValueObject, Value) {
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
            ValueObject::Hash(Arc::new(Mutex::new(map))),
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

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        match &mut data.data {
            ValueObject::Hash(hash) => {
                let mut hash = hash.lock();
                let hash_value = hash.get(&self.field);
                match hash_value {
                    Some(HashValue::Int(int)) => {
                        let new_int = int + self.value;
                        hash.insert(self.field.clone(), HashValue::Int(new_int));
                        (true, Value::Integer(new_int))
                    }
                    Some(HashValue::Str(_)) => (
                        false,
                        Value::Error("ERR hash value is not an integer".into()),
                    ),
                    None => {
                        hash.insert(self.field.clone(), HashValue::Int(self.value));
                        (true, Value::Integer(self.value))
                    }
                }
            }
            _ => (
                false,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let mut map = HashMap::new();
        map.insert(self.field, HashValue::Int(self.value));
        (
            ValueObject::Hash(Arc::new(Mutex::new(map))),
            Value::Integer(self.value),
        )
    }
}

impl MyCache {
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
                        None => Value::BulkString(None) ,
                        Some(value) => match value {
                            HashValue::Str(str) => Value::BulkString(Some(str.as_ref().clone())) ,
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

    pub fn h_set(&self, hset: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(hset, update)
    }
    pub fn h_incr(&self, h_incr: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(h_incr, update)
    }
}
