use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::core::value_object::{SortedSet, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, ZAddReq};
use parking_lot::Mutex;
use std::sync::Arc;

impl ComputeCommand for ZAddReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::ZAdd(self.clone())
    }

    fn mutate(self, value: &mut MyValue) -> (bool, Value) {
        match &value.data {
            ZSet(zset) => {
                let changed_count = zset.lock().zadd(self);
                (changed_count > 0, Value::Integer(changed_count))
            }
            _ => (false, Value::Error("zadd: key is not a zset".to_string())),
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let mut set = SortedSet::new();
        let changed_count = set.zadd(self);
        (
            ZSet(Arc::new(Mutex::new(set))),
            Value::Integer(changed_count),
        )
    }
}

impl MyCache {
    pub fn z_add(&self, zadd: ZAddReq, update: &mut Update) -> Value {
        self.execute_compute(zadd, update)
    }
}
