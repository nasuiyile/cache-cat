use crate::error::ProtocolError;
use crate::mocha::MochaOperation::Abort;
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::list::llen::LLenParams;
use crate::protocol::list::lrange::LRangeParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, LPopReq, LPushReq, RPushReq};
use bytes::Bytes;
use parking_lot::lock_api::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;

impl ComputeCommand for RPushReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::RPush(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let len = {
                    let mut list = data_arc.lock();
                    for element in self.elements {
                        list.push_back(element);
                    }
                    list.len() as i64
                };
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(len),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(Bytes::from_static(b"Key exists but is not a List")),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let deque: VecDeque<_> = VecDeque::from(self.elements);
        let len = deque.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(deque)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}

impl ComputeCommand for LPushReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LPush(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let len = {
                    let mut list = data_arc.lock();
                    for element in self.elements {
                        list.push_front(element);
                    }
                    list.len() as i64
                };
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(len),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(Bytes::from_static(b"Key exists but is not a List")),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let deque: VecDeque<_> = VecDeque::from(self.elements);
        let len = deque.len() as i64;
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::List(Arc::new(Mutex::new(deque)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len),
        )
    }
}

impl ComputeCommand for LPopReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::LPop(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::List(data_arc) => {
                let popped = {
                    let mut list = data_arc.lock();
                    list.pop_front()
                };
                match popped {
                    Some(value) => (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::BulkString(Some(value)),
                    ),
                    None => (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::BulkString(None),
                    ),
                }
            }
            _ => (
                Abort,
                Value::Error(Bytes::from_static(b"Key exists but is not a List")),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (Abort, Value::BulkString(None))
    }
}

impl ReadCommand for LRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::List(list) => {
                    let vec = crate::utils::lrange(&list.lock(), self.start, self.stop);
                    let mut array = Vec::new();
                    for x in vec {
                        let value = Value::BulkString(Some(x.clone()));
                        array.push(value);
                    }
                    Value::Array(Some(array))
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl ReadCommand for LLenParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Integer(0),
            Some(v) => match v.data {
                ValueObject::List(list) => Value::Integer(list.lock().len() as i64),
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl MyCache {
    pub fn l_range(&self, params: LRangeParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(params, db_number, read_clock)
    }
    pub fn l_len(&self, params: LLenParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(params, db_number, read_clock)
    }

    pub fn r_push(&self, param: RPushReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn l_push(&self, param: LPushReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn l_pop(&self, param: LPopReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
