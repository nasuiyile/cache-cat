use crate::error::ProtocolError;
use crate::protocol::zset::zrange::ZRangeParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::core::value_object::{SortedSet, ValueObject};
use crate::raft::types::entry::bae_operation::{BaseOperation, ZAddReq};
use parking_lot::Mutex;
use std::sync::Arc;
use crate::mocha::{EntryRef, ExpirePolicy, MochaOperation};

impl ComputeCommand for ZAddReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::ZAdd(self.clone())
    }

    fn mutate(
        self,
        entry: EntryRef<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ZSet(zset) => {
                let changed_count = zset.lock().zadd(self);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(changed_count),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error("zadd: key is not a zset".to_string()),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut set = SortedSet::new();
        let changed_count = set.zadd(self);
        (
            MochaOperation::Insert {
                value: MyValue::new(ZSet(Arc::new(Mutex::new(set)))),
                expire: ExpirePolicy::Persistent,
            },
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
