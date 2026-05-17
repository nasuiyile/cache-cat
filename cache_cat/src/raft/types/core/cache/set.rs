use crate::error::ProtocolError;
use crate::protocol::set::smembers::SMembersParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, SAddReq};
use moka::ops::compute::Op;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

impl ComputeCommand for SAddReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SAdd(self.clone())
    }

    fn mutate(self, mut data: MyValue) -> (Op<MyValue>, Value) {
        if let ValueObject::Set(map_arc) = &data.data {
            let mut count = 0;
            {
                let mut map = map_arc.lock();
                for v in &self.elements {
                    if map.insert(v.clone()) {
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
        let mut set = HashSet::new();
        let len = self.elements.len();
        for v in self.elements {
            set.insert(v);
        }
        (
            Op::Put(MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set))))),
            Value::Integer(len as i64),
        )
    }
}

impl MyCache {
    pub fn s_member(&self, param: SMembersParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&param.key) {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Set(set) => {
                    let guard = set.lock();
                    let mut array = Vec::new();
                    for member in guard.iter() {
                        array.push(Value::BulkString(Some(member.as_ref().clone())));
                    }
                    Value::Array(Some(array))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }

    pub fn s_add(&self, sadd: SAddReq, update: &mut Update) -> Value {
        self.execute_compute(sadd, update)
    }
}
