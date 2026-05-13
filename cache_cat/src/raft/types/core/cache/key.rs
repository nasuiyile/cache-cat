use crate::protocol::key::exists::ExistsParams;
use crate::protocol::key::expire::ExpireCondition;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{
    BaseOperation, DelReq, ExpireReq, InsertReq, PersistReq,
};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::AtomicRequest;

impl MyCache {
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
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut v = match cache.get(&persist.key) {
            Some(v) => v,
            None => return Value::Integer(0),
        };
        v.expires_at = 0;
        match update.update_type {
            UpdateType::None => {
                cache.insert(persist.key, v);
            }
            UpdateType::Snapshot(queue, write_clock) => {
                let key = persist.key.clone();
                v.version = v.version + 1;
                queue.push(AtomicRequest {
                    version: v.version,
                    request: BaseOperation::Persist(persist),
                    write_clock: *write_clock,
                });

                cache.insert(key, v);
            }
            UpdateType::CAS(cas_version) => {
                if *cas_version - 1 == v.version {
                    v.version += 1;
                    cache.insert(persist.key, v);
                }
            }
        }
        Value::Integer(1)
    }

    pub fn expire(&self, expire_req: ExpireReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let mut v = match cache.get(&expire_req.key) {
            Some(v) => v,
            None => return Value::Integer(0),
        };
        let should_update = match expire_req.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => v.expires_at == 0,
                ExpireCondition::Xx => v.expires_at != 0,
                ExpireCondition::Gt => v.expires_at != 0 && v.expires_at <= expire_req.expires_at,
                ExpireCondition::Lt => v.expires_at != 0 && v.expires_at >= expire_req.expires_at,
            },
        };
        if !should_update {
            return Value::Integer(0);
        }

        v.expires_at = expire_req.expires_at;
        match update.update_type {
            UpdateType::None => {
                cache.insert(expire_req.key, v);
            }
            UpdateType::Snapshot(queue, write_clock) => {
                let key = expire_req.key.clone();
                v.version = v.version + 1;
                queue.push(AtomicRequest {
                    version: v.version,
                    request: BaseOperation::Expire(expire_req),
                    write_clock: *write_clock,
                });

                cache.insert(key, v);
            }
            UpdateType::CAS(cas_version) => {
                if *cas_version - 1 == v.version {
                    v.version += 1;
                    cache.insert(expire_req.key, v);
                }
            }
        }
        Value::Integer(1)
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

            UpdateType::Snapshot(queue, write_clock) => {
                // 计算 version
                let version = if let Some(entry) = cache.get(&del_req.key) {
                    entry.version + 1
                } else {
                    1
                };
                queue.push(AtomicRequest {
                    version,
                    request: BaseOperation::Del(del_req.clone()),
                    write_clock: *write_clock,
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
            UpdateType::Snapshot(queue, write_clock) => {
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
                        write_clock: *write_clock,
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
