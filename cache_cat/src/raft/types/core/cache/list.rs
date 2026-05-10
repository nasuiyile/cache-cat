use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, LPushReq};
use parking_lot::lock_api::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

impl ComputeCommand for LPushReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LPush(self.clone())
    }

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        match &data.data {
            ValueObject::List(data_arc) => {
                let mut list = data_arc.lock();

                for element in self.elements {
                    list.push_front(element);
                }

                let len = list.len() as i64;
                (true, Value::Integer(len))
            }
            _ => (
                false,
                Value::Error("Key exists but is not a List".to_string()),
            ),
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let deque: VecDeque<_> = VecDeque::from(self.elements);
        let len = deque.len() as i64;

        (
            ValueObject::List(Arc::new(Mutex::new(deque))),
            Value::Integer(len),
        )
    }
}

impl MyCache {
    pub fn l_push(&self, l_push: LPushReq, update: &mut Update) -> Value {
        self.execute_compute(l_push, update)
    }
}
