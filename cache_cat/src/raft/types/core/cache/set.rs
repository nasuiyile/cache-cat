use crate::error::ProtocolError;
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::set::smembers::SMembersParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, SAddReq, SRemReq};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

impl ComputeCommand for SRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Set(set) => {
                let mut set = set.lock();
                let mut deleted_count = 0;
                for member in &self.members {
                    if set.remove(member) {
                        deleted_count += 1;
                    }
                }
                let is_empty = set.is_empty();
                drop(set);

                if deleted_count == 0 {
                    return (MochaOperation::Abort, Value::Integer(0));
                }
                if is_empty {
                    return (MochaOperation::Remove, Value::Integer(deleted_count));
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

impl ComputeCommand for SAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SAdd(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Set(set) => {
                let mut count = 0;
                {
                    let mut set = set.lock();
                    for v in &self.elements {
                        if set.insert(v.clone()) {
                            count += 1;
                        }
                    }
                }
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
        let mut set = HashSet::new();
        let len = self.elements.len();
        for v in self.elements {
            set.insert(v);
        }
        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Set(Arc::new(Mutex::new(set)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(len as i64),
        )
    }
}

impl ReadCommand for SMembersParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
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
}

impl MyCache {
    pub fn s_member(
        &self,
        param: SMembersParams,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        self.execute_read(param, db_number, read_clock)
    }

    pub fn s_rem(&self, param: SRemReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn s_add(&self, param: SAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
