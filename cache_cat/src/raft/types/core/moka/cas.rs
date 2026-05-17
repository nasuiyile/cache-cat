use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::response_value::Value::Integer;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::AtomicRequest;
use moka::ops::compute::{CompResult, Op};
use std::sync::Arc;

pub trait ComputeCommand: Send + 'static {
    fn key(&self) -> Arc<Vec<u8>>;

    fn into_base_op(self) -> BaseOperation;

    /// 返回: (是否修改, 返回值)
    fn mutate(self, value: MyValue) -> (Op<MyValue>, Value);

    /// 返回: (初始化值, 返回值)
    fn init(self) -> (Op<MyValue>, Value);
}

impl MyCache {
    pub fn execute_compute<C>(&self, cmd: C, update: &mut Update) -> Value
    where
        C: ComputeCommand + Clone,
    {
        let cache = match self.databases.get(update.db_number as usize) {
            None => return Value::error("Key not found"),
            Some(v) => &v.cache,
        };

        let key = cmd.key();
        let mut return_value = Integer(0);

        let result = match update.update_type {
            UpdateType::None => cache.entry(key).and_compute_with(|maybe_entry| {
                let cmd = cmd.clone();
                match maybe_entry {
                    Some(entry) => {
                        let value = entry.into_value();
                        let (changed, res) = cmd.mutate(value);
                        return_value = res;
                        changed
                    }
                    None => {
                        let (new_obj, res) = cmd.init();
                        return_value = res;
                        new_obj
                    }
                }
            }),
            UpdateType::Snapshot(queue) => cache.entry(key).and_compute_with(|maybe_entry| {
                let cmd_copy = cmd.clone();
                let mut next_version = 1;

                let op = match maybe_entry {
                    Some(entry) => {
                        let value = entry.into_value();
                        let (changed, res) = cmd.mutate(value);
                        return_value = res;
                        match changed {
                            Op::Nop => Op::Nop,
                            Op::Put(mut value) => {
                                value.version += 1;
                                next_version = value.version;
                                Op::Put(value)
                            }
                            Op::Remove => Op::Remove,
                        }
                    }
                    None => {
                        let (new_obj, res) = cmd.init();
                        return_value = res;

                        new_obj
                    }
                };

                queue.push(AtomicRequest {
                    request: cmd_copy.into_base_op(),
                    version: next_version,
                    write_clock: update.write_clock,
                });
                op
            }),

            UpdateType::CAS(cas_version) => {
                let expected_version = *cas_version - 1;
                cache.entry(key).and_compute_with(|maybe_entry| {
                    let cmd = cmd.clone();
                    match maybe_entry {
                        Some(entry) => {
                            let mut value = entry.into_value();

                            if value.version != expected_version {
                                return_value = Integer(0);
                                return Op::Nop;
                            }

                            let (changed, res) = cmd.mutate(value);
                            return_value = res;
                            match changed {
                                Op::Nop => Op::Nop,
                                Op::Put(mut value) => {
                                    value.version += 1;
                                    Op::Put(value)
                                }
                                Op::Remove => Op::Remove,
                            }
                        }
                        None => {
                            let (new_obj, res) = cmd.init();
                            return_value = res;
                            new_obj
                        }
                    }
                })
            }
        };

        match result {
            CompResult::StillNone(_) => Value::Error("Key not found".into()),
            _ => return_value,
        }
    }
}
