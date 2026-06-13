use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::key::del::DelParams;
use crate::protocol::key::exists::ExistsParams;
use crate::protocol::key::expire::ExpireCondition;
use crate::protocol::key::rename::RenameParams;
use crate::protocol::key::renamenx::RenameNxParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{
    BaseOperation, DelReq, ExpireReq, InsertReq, PersistReq,
};
use crate::raft::types::entry::request::AtomicRequest;
use std::sync::Arc;

impl ComputeCommand for ExpireReq {
    fn key(&self) -> Arc<Vec<u8>> {
        Arc::from(self.key.clone())
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Expire(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expires_at = self.expires_at + write_clock;
        let should_update = match self.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => entry.expire_at.is_none(),
                ExpireCondition::Xx => entry.expire_at.is_some(),
                ExpireCondition::Gt => {
                    match entry.expire_at {
                        None => false,                       // 无过期 = 无穷大，新过期不可能大于无穷大
                        Some(expire) => expire < expires_at, // 旧 < 新，即新 > 旧
                    }
                }
                ExpireCondition::Lt => {
                    match entry.expire_at {
                        None => true,                        // 无过期 = 无穷大，新过期一定小于无穷大
                        Some(expire) => expire > expires_at, // 旧 > 新，即新 < 旧
                    }
                }
            },
        };
        if !should_update {
            return (MochaOperation::Abort, Value::Integer(0));
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: ExpirePolicy::Absolute(expires_at),
            },
            Value::Integer(1),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}
impl ComputeCommand for PersistReq {
    fn key(&self) -> Arc<Vec<u8>> {
        Arc::from(self.key.clone())
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Persist(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        if entry.expire_at.is_none() {
            return (MochaOperation::Abort, Value::Integer(0));
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(1),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}

impl ComputeCommand for InsertReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Insert(self.clone())
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
            data: self.value.clone(),
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
        let my_value = match cached.get_entry(&params.key) {
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
            value: my_value.value.data,
            expires_at: my_value.expire_at.unwrap_or(0),
        };
        self.insert(insert, update);
        Value::ok()
    }

    pub fn redis_rename_nx(
        &self,
        params: RenameNxParams,
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
        // Check if new_key already exists - if so, return 0 without renaming
        if cached.get_entry(&params.new_key).is_some() {
            return Value::Integer(0);
        }
        // Check if source key exists
        let my_value = match cached.get_entry(&params.key) {
            None => return Value::Error("no such key".to_string()),
            Some(value) => value,
        };
        // Delete the old key
        let del = DelReq {
            key: Arc::from(params.key),
        };
        self.del(del, update);

        // Insert with the new key
        let new_key: Arc<Vec<u8>> = Arc::from(params.new_key);
        let insert = InsertReq {
            key: new_key.clone(),
            value: my_value.value.data,
            expires_at: my_value.expire_at.unwrap_or(0),
        };
        self.insert(insert, update);

        // Return 1 to indicate successful rename
        Value::Integer(1)
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
        self.execute_compute(insert_req, update)
    }
}
