use crate::error::ProtocolError;
use crate::protocol::zset::zrange::ZRangeParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::core::value_object::{SortedSet, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, ZAddReq};
use moka::ops::compute::Op;
use parking_lot::Mutex;
use std::sync::Arc;

impl ComputeCommand for ZAddReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::ZAdd(self.clone())
    }

    fn mutate(self, mut value: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        match &value.data {
            ZSet(zset) => {
                let changed_count = zset.lock().zadd(self);
                (Op::Put(value), Value::Integer(changed_count))
            }
            _ => (Op::Nop, Value::Error("zadd: key is not a zset".to_string())),
        }
    }

    fn init(self) -> (Op<MyValue>, Value) {
        let mut set = SortedSet::new();
        let changed_count = set.zadd(self);
        (
            Op::Put(MyValue::new(ZSet(Arc::new(Mutex::new(set))))),
            Value::Integer(changed_count),
        )
    }
}

impl MyCache {
    pub fn z_range(&self, params: ZRangeParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        match cache.get(&params.key) {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ZSet(list) => {
                    let res = list
                        .lock()
                        .zrange(params.start, params.stop, params.with_scores);
                    let mut vec = Vec::new();
                    for v in res {
                        vec.push(Value::BulkString(Some(v)))
                    }
                    Value::Array(Some(vec))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
    pub fn z_add(&self, zadd: ZAddReq, update: &mut Update) -> Value {
        self.execute_compute(zadd, update)
    }
}
