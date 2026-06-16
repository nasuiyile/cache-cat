use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hgetall::HGetAllParams;
use crate::protocol::hash::hkeys::HKeysParams;
use crate::protocol::hash::hmget::HMGetParams;
use crate::protocol::hash::hvals::HValsParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, HDelReq, HIncrReq, HSetReq};
use crate::utils::parse_i64;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

impl ComputeCommand for HSetReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HSet(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut count = 0;
                let mut map = hash.lock();
                for (k, v) in &self.elements {
                    let value = parse_i64(v)
                        .map(HashValue::Int)
                        .unwrap_or_else(|| HashValue::Str(v.clone()));
                    if map.insert(k.clone(), value).is_none() {
                        count += 1;
                    }
                }
                drop(map);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(count),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
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
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len as i64),
        )
    }
}

impl ComputeCommand for HIncrReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HIncr(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut map = hash.lock();
                let result = match map.get(&self.field) {
                    Some(HashValue::Int(int)) => {
                        let new_int = *int + self.value;
                        map.insert(self.field.clone(), HashValue::Int(new_int));
                        Value::Integer(new_int)
                    }
                    Some(HashValue::Str(_)) => {
                        return (
                            MochaOperation::Abort,
                            Value::Error("ERR hash value is not an integer".into()),
                        );
                    }
                    None => {
                        map.insert(self.field.clone(), HashValue::Int(self.value));
                        Value::Integer(self.value)
                    }
                };
                drop(map);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    result,
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut map = HashMap::new();
        map.insert(self.field, HashValue::Int(self.value));
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Hash(Arc::new(Mutex::new(map)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(self.value),
        )
    }
}

impl ComputeCommand for HDelReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::HDel(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Hash(hash) => {
                let mut map = hash.lock();
                let mut deleted_count = 0;
                for field in &self.fields {
                    if map.remove(field).is_some() {
                        deleted_count += 1;
                    }
                }
                drop(map);
                if deleted_count == 0 {
                    return (MochaOperation::Abort, Value::Integer(0));
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(deleted_count),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}

impl ReadCommand for HMGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let results: Vec<Value> = self
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
}

impl ReadCommand for HKeysParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let mut result = Vec::with_capacity(guard.len());
                    for (field, _) in guard.iter() {
                        result.push(Value::BulkString(Some(field.as_ref().clone())));
                    }
                    Value::Array(Some(result))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}
impl ReadCommand for HValsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();

                    let mut result = Vec::with_capacity(guard.len());

                    for value in guard.values() {
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };

                        result.push(Value::BulkString(Some(value_bytes)));
                    }

                    Value::Array(Some(result))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetAllParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Map(Vec::new()),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let mut result = Vec::with_capacity(guard.len());
                    for (field, value) in guard.iter() {
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };
                        result.push((
                            Value::BulkString(Some(field.as_ref().clone())),
                            Value::BulkString(Some(value_bytes)),
                        ));
                    }
                    Value::Map(result)
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&self.field);
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
}
impl MyCache {
    pub fn h_m_get(&self, param: HMGetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_keys(&self, param: HKeysParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_vals(&self, param: HValsParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_get_all(
        &self,
        param: HGetAllParams,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn h_get(&self, param: HGetParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(param, db_number, read_clock)
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
