//! Internal single-key INSERT used to apply computed command results.
// 只提供给内部使用
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::{BaseOperation, InsertReq};
use bytes::Bytes;

impl ComputeCommand for InsertReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        // Only called when recording a snapshot operation. The live cache and
        // the queued INSERT must not share mutable containers.
        BaseOperation::Insert(InsertReq {
            value: self.value.snapshot_clone(),
            ..self
        })
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        // 版本递增
        let new_version = entry.value.version + 1;
        let expire = if self.expires_at == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.expires_at)
        };
        let new_value = MyValue {
            version: new_version,
            data: self.value,
        };
        (
            MochaOperation::Insert {
                value: new_value,
                expire,
            },
            Value::ok(),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let expire = if self.expires_at == 0 {
            ExpirePolicy::Persistent
        } else {
            ExpirePolicy::Absolute(self.expires_at)
        };
        let value = MyValue {
            version: 1,
            data: self.value,
        };
        (MochaOperation::Insert { value, expire }, Value::ok())
    }
}
