use crate::protocol::key::del::{DelParams, DelReq};
use crate::protocol::key::expire::ExpireReq;
use crate::protocol::key::flushall::FlushAllReq;
use crate::protocol::key::flushdb::FlushDBReq;
use crate::protocol::key::keys::KeysParams;
use crate::protocol::key::persist::PersistReq;
use crate::protocol::key::pexpire::PExpireReq;
use crate::protocol::key::unlink::{UnlinkParams, UnlinkReq};
use crate::protocol::set::spop::SPopReq;
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::{BaseOperation, InsertReq};
use crate::raft::types::entry::request::AtomicRequest;

impl MyCache {
    pub fn redis_del(&self, params: DelParams, update: &mut Update<'_>, external: bool) -> Value {
        let mut count = 0;
        let _exclusive_lock = if external {
            Some(self.read_lock.write())
        } else {
            None
        };

        for key in params.keys {
            let del = DelReq { key };
            match self.del(del, update) {
                Value::Error(err) => return Value::Error(err),
                Value::Integer(num) => count += num,
                _ => {}
            }
        }
        Value::Integer(count)
    }

    pub fn persist(&self, persist: PersistReq, update: &mut Update) -> Value {
        self.execute_compute(persist, update)
    }
    pub fn dbsize(&self, db_number: u16) -> Value {
        let cache = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => &cache.mocha,
        };
        Value::Integer(cache.len() as i64)
    }

    pub fn expire(&self, param: ExpireReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn p_expire(&self, param: PExpireReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn redis_unlink(
        &self,
        params: UnlinkParams,
        update: &mut Update<'_>,
        external: bool,
    ) -> Value {
        let mut count = 0;
        let _exclusive_lock = if external {
            Some(self.read_lock.write())
        } else {
            None
        };
        for key in params.keys {
            let del = UnlinkReq { key };
            match self.unlink(del, update) {
                Value::Error(err) => return Value::Error(err),
                Value::Integer(num) => count += num,
                _ => {}
            }
        }
        Value::Integer(count)
    }

    pub fn unlink(&self, del_req: UnlinkReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => &cache.mocha,
        };
        //是否删除了元素
        match update.update_type {
            UpdateType::None => {
                let existed = cache.unlink(&del_req.key);
                if existed {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }

            UpdateType::Snapshot(queue) => {
                // 计算 version
                let version = if let Some(entry) = cache.get(&del_req.key) {
                    entry.version.wrapping_add(1)
                } else {
                    1
                };
                queue.push(AtomicRequest {
                    version,
                    request: BaseOperation::Unlink(del_req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });

                let existed = cache.unlink(&del_req.key);
                if existed {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }
            UpdateType::CAS(cas_version) => {
                if let Some(entry) = cache.get(&del_req.key)
                    && entry.version == cas_version.wrapping_sub(1)
                {
                    cache.remove(&del_req.key);
                    return Value::Integer(1);
                }
                Value::Integer(0)
            }
        }
    }

    pub fn del(&self, del_req: DelReq, update: &mut Update) -> Value {
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => &cache.mocha,
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
                    entry.version.wrapping_add(1)
                } else {
                    1
                };
                queue.push(AtomicRequest {
                    version,
                    request: BaseOperation::Del(del_req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });

                let existed = cache.remove(&del_req.key);
                if existed.is_some() {
                    Value::Integer(1)
                } else {
                    Value::Integer(0)
                }
            }
            UpdateType::CAS(cas_version) => {
                if let Some(entry) = cache.get(&del_req.key)
                    && entry.version == cas_version.wrapping_sub(1)
                {
                    cache.remove(&del_req.key);
                    return Value::Integer(1);
                }
                Value::Integer(0)
            }
        }
    }

    pub fn flush_db(&self, req: FlushDBReq, update: &mut Update) -> Value {
        let _lock = self.read_lock.write();
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => &cache.mocha,
        };
        match update.update_type {
            UpdateType::None => {
                cache.clear();
            }
            UpdateType::Snapshot(queue) => {
                queue.push(AtomicRequest {
                    version: 1,
                    request: BaseOperation::FlushDB(req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                cache.clear();
            }
            UpdateType::CAS(_) => {
                cache.clear();
            }
        }
        Value::ok()
    }

    pub fn flush_all(&self, req: FlushAllReq, update: &mut Update) -> Value {
        let _lock = self.read_lock.write();
        match update.update_type {
            UpdateType::None => {
                for database in &self.databases {
                    database.mocha.clear();
                }
            }
            UpdateType::Snapshot(queue) => {
                queue.push(AtomicRequest {
                    version: 1,
                    request: BaseOperation::FlushAll(req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                for database in &self.databases {
                    database.mocha.clear();
                }
            }
            UpdateType::CAS(_) => {
                for database in &self.databases {
                    database.mocha.clear();
                }
            }
        }
        Value::ok()
    }

    pub fn s_pop(&self, param: SPopReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn insert(&self, insert_req: InsertReq, update: &mut Update) -> Value {
        self.execute_compute(insert_req, update)
    }

    pub fn keys(&self, param: KeysParams, db_number: u16, read_clock: Option<u64>) -> Value {
        let cached = match self.get_cache(db_number) {
            Err(err) => return err,
            Ok(cache) => cache,
        };
        let keys = cached.mocha.keys(&param.pattern, read_clock);
        let values: Vec<Value> = keys
            .into_iter()
            .map(|b| Value::BulkString(Some(b)))
            .collect();
        Value::Array(Some(values))
    }
}
