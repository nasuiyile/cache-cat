use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::key::del::DelReq;
use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::mocha::request_handler::base_request;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::{BaseOperation, InsertReq};
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

/// 多 key 读取 + 多 key 写入的确定性计算命令。
/// `mutate_writes` 返回已经计算完成的写集；快照回放只记录这些具体写入，
/// 因而不会再次读取可能已经变化的源 key。
pub trait MultiReadComputeCommand: Send + 'static {
    /// Return every key that participates in the read phase, in the order
    /// expected by `mutate_writes`.
    fn read_keys(&self) -> impl Iterator<Item = &Bytes>;

    /// Compute the complete deterministic write set. The executor applies
    /// every returned operation in order and records those concrete
    /// operations during snapshotting. Do not mutate the input entries in
    /// place. Return an empty write set on errors or when no change is needed.
    fn mutate_writes(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        write_clock: u64,
    ) -> (Vec<ComputedWrite>, Value);
}

/// A concrete change to one key. INSERT versions are assigned by the
/// single-key writer from the destination entry, never from the source value.
#[derive(Debug, Clone)]
pub struct ComputedWrite {
    pub key: Bytes,
    pub operation: MochaOperation<MyValue>,
}

impl ComputedWrite {
    fn into_base_operation(self, write_clock: u64) -> Option<BaseOperation> {
        let key = self.key;
        match self.operation {
            MochaOperation::Insert { value, expire } => {
                let expires_at = match expire {
                    ExpirePolicy::Absolute(at) => at,
                    ExpirePolicy::Persistent => 0,
                    ExpirePolicy::Ttl(ttl) => write_clock.saturating_add(ttl),
                };
                Some(BaseOperation::Insert(InsertReq {
                    key,
                    value: value.data,
                    expires_at,
                }))
            }
            MochaOperation::Remove => Some(BaseOperation::Del(DelReq { key })),
            MochaOperation::Abort => None,
        }
    }
}

impl MyCache {
    // Reuse the single-key writers for versions and snapshot/CAS handling.
    pub(crate) fn execute_computed_writes(&self, writes: Vec<ComputedWrite>, update: &mut Update) {
        if writes
            .iter()
            .any(|write| matches!(write.operation, MochaOperation::Abort))
        {
            return;
        }
        for write in writes {
            if let Some(request) = write.into_base_operation(update.write_clock) {
                base_request(self, request, update);
            }
        }
    }

    /// Callers serialize writes with `write_lock`. Commands that can write
    /// multiple keys also hold `read_lock.write()` at the outer request boundary
    /// so EXEC/Lua can reuse their existing lock without acquiring it twice.
    pub fn execute_multi_read_compute<C>(&self, cmd: C, update: &mut Update) -> Value
    where
        C: MultiReadComputeCommand,
    {
        // 所有 MultiReadComputeCommand命令，都会被内部执行为baseoperation。因此重放阶段不会存在。
        if matches!(update.update_type, UpdateType::CAS(_)) {
            return Value::error("multi-key commands must replay their computed writes");
        }
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(v) => &v.mocha,
        };
        let read_entries: Vec<Option<EntrySnapshot<MyValue>>> =
            cmd.read_keys().map(|key| cache.get_entry(key)).collect();
        let (writes, return_value) = cmd.mutate_writes(read_entries, update.write_clock);
        self.execute_computed_writes(writes, update);
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
