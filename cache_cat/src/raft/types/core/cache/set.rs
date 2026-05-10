use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, SAddReq};
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

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        if let ValueObject::Set(map_arc) = &data.data {
            let mut count = 0;
            let mut map = map_arc.lock();
            for v in &self.elements {
                if map.insert(v.clone()) {
                    count += 1;
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
        let mut set = HashSet::new();
        let len = self.elements.len();
        for v in self.elements {
            set.insert(v);
        }
        (
            ValueObject::Set(Arc::new(Mutex::new(set))),
            Value::Integer(len as i64),
        )
    }
}

impl MyCache {
    pub fn s_add(&self, sadd: SAddReq,update: &mut Update) -> Value {
        self.execute_compute(sadd, update)
    }
}
