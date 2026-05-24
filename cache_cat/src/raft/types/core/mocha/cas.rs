use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::response_value::Value::Integer;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::AtomicRequest;
use std::sync::Arc;

pub trait ComputeCommand: Send + 'static {
    fn key(&self) -> Arc<Vec<u8>>;

    fn into_base_op(self) -> BaseOperation;

    /// 返回: (是否修改, 返回值)
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value);

    /// 返回: (初始化值, 返回值)
    fn init(self) -> (MochaOperation<MyValue>, Value);
}

impl MyCache {
    pub fn execute_compute<C>(&self, cmd: C, update: &mut Update) -> Value
    where
        C: ComputeCommand + Clone,
    {
        let cache = match self.databases.get(update.db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };

        let key = cmd.key();
        let option = cache.get_entry(&key);
        let entry = match option {
            None => {
                let (new_obj, res) = cmd.init();
                match new_obj {
                    MochaOperation::Insert { value, expire } => {
                        cache.insert_entry(key, value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&key);
                    }
                    MochaOperation::Abort => {
                        return Value::error("Key not found");
                    }
                }

                return res;
            }
            Some(v) => v,
        };
        let mut return_value = Integer(0);

        match update.update_type {
            UpdateType::None => {
                let (changed, res) = cmd.mutate(entry, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        cache.insert_entry(key, value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&key);
                    }
                    MochaOperation::Abort => {}
                }
            }
            UpdateType::Snapshot(queue) => {
                let cmd_copy = cmd.clone();
                let mut next_version = 1;
                let (changed, res) = cmd.mutate(entry, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        next_version = value.version + 1;
                        cache.insert_entry(key, value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&key);
                    }
                    MochaOperation::Abort => {}
                }

                queue.push(AtomicRequest {
                    request: cmd_copy.into_base_op(),
                    version: next_version,
                    write_clock: update.write_clock,
                });
            }
            UpdateType::CAS(cas_version) => {
                let expected_version = *cas_version - 1;
                if entry.value.version != expected_version {
                    return_value = Integer(0);
                }
                let (changed, res) = cmd.mutate(entry, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { mut value, expire } => {
                        value.version += 1;
                        cache.insert_entry(key, value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&key);
                    }
                    MochaOperation::Abort => {}
                }
            }
        };

        return_value
    }
}
