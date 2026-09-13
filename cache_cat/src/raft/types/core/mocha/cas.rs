use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::AtomicRequest;
use bytes::Bytes;

pub trait ComputeCommand: Send + 'static {
    fn key(&self) -> &Bytes;

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

/// 多 key 读取 + 单 key 写入的 ComputeCommand
pub trait MultiReadComputeCommand: Send + 'static {
    fn write_key(&self) -> &Bytes;
    fn read_keys(&self) -> &[Bytes];
    fn into_base_op(self) -> BaseOperation;

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value);
}

impl MyCache {
    pub fn execute_multi_read_compute<C>(&self, cmd: C, update: &mut Update) -> Value
    where
        C: MultiReadComputeCommand + Clone,
    {
        let cache = match self.databases.get(update.db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };
        let read_entries: Vec<Option<EntrySnapshot<MyValue>>> = cmd
            .read_keys()
            .iter()
            .map(|key| cache.get_entry(key))
            .collect();
        let write_key = cmd.write_key().clone();

        let return_value;

        match update.update_type {
            UpdateType::None => {
                let (changed, res) = cmd.mutate(read_entries, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        cache.insert_entry(write_key.clone(), value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&write_key);
                    }
                    MochaOperation::Abort => {}
                }
            }
            UpdateType::Snapshot(queue) => {
                let cmd_copy = cmd.clone();
                let mut next_version = 1;
                let (changed, res) = cmd.mutate(read_entries, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        next_version = value.version;
                        cache.insert_entry(write_key.clone(), value, expire);
                    }
                    MochaOperation::Remove => {
                        cache.remove(&write_key);
                    }
                    MochaOperation::Abort => {}
                }

                queue.push(AtomicRequest {
                    request: cmd_copy.into_base_op(),
                    version: next_version,
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
            }
            UpdateType::CAS(cas_version) => {
                let expected_version = cas_version.saturating_sub(1);
                if let Some(entry) = cache.get_entry(&write_key) {
                    if entry.value.version != expected_version {
                        return Value::Null;
                    }
                    let (changed, res) = cmd.mutate(read_entries, update.write_clock);
                    match changed {
                        MochaOperation::Insert { value, expire } => {
                            cache.insert_entry(write_key, value, expire);
                        }
                        MochaOperation::Remove => {
                            cache.remove(&write_key);
                        }
                        MochaOperation::Abort => {}
                    }
                    return res;
                }
                if *cas_version != 1 {
                    return Value::Null;
                }
                let (changed, res) = cmd.mutate(read_entries, update.write_clock);
                if let MochaOperation::Insert { value, expire } = changed {
                    cache.insert_entry(write_key, value, expire);
                }
                return res;
            }
        };

        return_value
    }

    pub fn execute_compute<C>(&self, cmd: C, update: &mut Update) -> Value
    where
        C: ComputeCommand + Clone,
    {
        let cache = match self.databases.get(update.db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.mocha,
        };

        let key = cmd.key().clone();
        let option = cache.get_entry(&key);
        if option.is_none() {
            match update.update_type {
                UpdateType::CAS(version) if *version != 1 => return Value::Null,
                UpdateType::CAS(_) | UpdateType::None | UpdateType::Snapshot(_) => {}
            }
            let cmd_copy = cmd.clone();
            let (new_obj, res) = cmd.init();
            let mut inserted = false;
            if let MochaOperation::Insert { value, expire } = new_obj {
                cache.insert_entry(key.clone(), value, expire);
                inserted = true;
            }
            if let UpdateType::Snapshot(queue) = update.update_type {
                queue.push(AtomicRequest {
                    request: cmd_copy.into_base_op(),
                    version: 1,
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
            }
            if !inserted && matches!(res, Value::Error(_)) {
                return res;
            }
            return res;
        }
        let entry = option.expect("checked above");
        let return_value;

        match update.update_type {
            UpdateType::None => {
                let (changed, res) = cmd.mutate(entry, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        cache.insert_entry(key.clone(), value, expire);
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
                        //版本号为当前数据的版本号 +1
                        next_version = value.version;
                        cache.insert_entry(key.clone(), value, expire);
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
                    db_number: update.db_number,
                });
            }
            UpdateType::CAS(cas_version) => {
                let expected_version = cas_version.saturating_sub(1);
                if entry.value.version != expected_version {
                    // The snapshot already contains this write (or a later
                    // one), so replay is intentionally a no-op.
                    return Value::Null;
                }
                let (changed, res) = cmd.mutate(entry, update.write_clock);
                return_value = res;
                match changed {
                    MochaOperation::Insert { value, expire } => {
                        cache.insert_entry(key.clone(), value, expire);
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
