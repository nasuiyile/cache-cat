use crate::protocol::key::del::DelParams;
use crate::protocol::key::exists::ExistsParams;
use crate::protocol::key::expire::ExpireCondition;
use crate::protocol::key::rename::RenameParams;
use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{
    BaseOperation, DelReq, ExpireReq, InsertReq, PersistReq,
};
use crate::raft::types::entry::request::AtomicRequest;
use moka::ops::compute::Op;
use std::sync::Arc;

impl ComputeCommand for ExpireReq {
    fn key(&self) -> Arc<Vec<u8>> {
        Arc::from(self.key.clone())
    }
    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Expire(self.clone())
    }
    fn mutate(self, mut data: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        let expires_at = self.expires_at + write_clock;
        let should_update = match self.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => data.expires_at == 0,
                ExpireCondition::Xx => data.expires_at != 0,
                ExpireCondition::Gt => data.expires_at != 0 && data.expires_at <= expires_at,
                ExpireCondition::Lt => data.expires_at != 0 && data.expires_at >= expires_at,
            },
        };
        if !should_update {
            return (Op::Nop, Value::Integer(0));
        }
        data.expires_at = expires_at;
        (Op::Put(data), Value::Integer(1))
    }
    fn init(self) -> (Op<MyValue>, Value) {
        (Op::Nop, Value::Integer(0))
    }
}

impl ComputeCommand for PersistReq {
    fn key(&self) -> Arc<Vec<u8>> {
        Arc::from(self.key.clone())
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Persist(self.clone())
    }

    fn mutate(self, mut value: MyValue, write_clock: u64) -> (Op<MyValue>, Value) {
        if value.expires_at == 0 {
            return (Op::Nop, Value::Integer(0));
        }
        value.expires_at = 0;
        (Op::Put(value), Value::Integer(1))
    }

    fn init(self) -> (Op<MyValue>, Value) {
        (Op::Nop, Value::Integer(0))
    }
}


impl MyCache {
    pub fn redis_rename(
        &self,
        params: RenameParams,
        update: &mut Update<'_>,
        external: bool,
    ) -> Value {
        if external {
            let _exclusive_lock = self.read_lock.write();
        }
        let cached = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let my_value = match cached.get(&params.key) {
            None => return Value::Error("no such key".to_string()),
            Some(value) => value,
        };
        let del = DelReq {
            key: Arc::from(params.key),
        };
        self.del(del, update);
        let new_key: Arc<Vec<u8>> = Arc::from(params.new_key);
        let insert = InsertReq {
            key: new_key.clone(),
            value: my_value.data,
            expires_at: my_value.expires_at,
        };
        self.insert(insert, update);
        Value::ok()
    }

    pub fn redis_del(&self, params: DelParams, update: &mut Update<'_>, external: bool) -> Value {
        let mut count = 0;
        if external {
            let _exclusive_lock = self.read_lock.write();
        }
        for key in params.keys {
            let del = DelReq {
                key: Arc::from(key),
            };
            match self.del(del, update) {
                Value::Error(err) => return Value::Error(err),
                Value::Integer(num) => count = count + num,
                _ => {}
            }
        }
        Value::Integer(count)
    }

    pub fn exists(&self, exists_params: ExistsParams, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut count = 0;
        for key in exists_params.keys {
            if cache.contains_key(&key) {
                count += 1;
            }
        }
        Value::Integer(count)
    }

    pub fn persist(&self, persist: PersistReq, update: &mut Update) -> Value {
        self.execute_compute(persist, update)
    }

    pub fn expire(&self, param: ExpireReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn del(&self, del_req: DelReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        //是否删除了元素
        match update.update_type {
            UpdateType::None => {
                let existed = cache.remove(&del_req.key);
                if existed.is_some() {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }

            UpdateType::Snapshot(queue) => {
                // 计算 version
                let version = if let Some(entry) = cache.get(&del_req.key) {
                    entry.version + 1
                } else {
                    1
                };
                queue.push(AtomicRequest {
                    version,
                    request: BaseOperation::Del(del_req.clone()),
                    write_clock: update.write_clock,
                });

                let existed = cache.remove(&del_req.key);
                if existed.is_some() {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }
            UpdateType::CAS(cas_version) => {
                if let Some(entry) = cache.get(&del_req.key) {
                    if entry.version == *cas_version - 1 {
                        cache.remove(&del_req.key);
                        return Value::Integer(1);
                    }
                }
                Value::Integer(0)
            }
        }
    }

    pub fn insert(&self, insert_req: InsertReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut value = MyValue {
            version: 1,
            expires_at: insert_req.expires_at,
            data: insert_req.value.clone(),
        };
        match update.update_type {
            UpdateType::None => {
                cache.insert(insert_req.key, value);
            }
            UpdateType::Snapshot(queue) => {
                let key = insert_req.key.clone();
                cache.entry(key).and_upsert_with(|old_entry| {
                    value.version = if let Some(entry) = old_entry {
                        entry.into_value().version + 1
                    } else {
                        1
                    };
                    queue.push(AtomicRequest {
                        version: value.version,
                        request: BaseOperation::Insert(insert_req),
                        write_clock: update.write_clock,
                    });
                    value
                });
            }
            UpdateType::CAS(cas_version) => {
                let key = insert_req.key.clone();
                cache.entry(key).and_upsert_with(|maybe_entry| {
                    if let Some(entry) = maybe_entry {
                        let current_val = entry.value();
                        // 核心逻辑：只有传入的 version 与缓存中的 version 相同时才允许更新
                        if *cas_version - 1 == current_val.version {
                            value.version += 1;
                            value
                        } else {
                            // 版本不匹配，直接返回旧值（即不更新）
                            current_val.clone()
                        }
                    } else {
                        let new_data = insert_req.value;
                        let ttl = insert_req.expires_at;
                        MyValue {
                            data: new_data,
                            expires_at: ttl,
                            version: 1, // 初始版本
                        }
                    }
                });
            }
        }
        Value::ok()
    }
}
