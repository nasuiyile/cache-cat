use crate::protocol::key::del::{DelParams, DelReq};
use crate::protocol::key::expire::ExpireReq;
use crate::protocol::key::flushall::FlushAllReq;
use crate::protocol::key::flushdb::FlushDBReq;
use crate::protocol::key::keys::KeysParams;
use crate::protocol::key::persist::PersistReq;
use crate::protocol::key::pexpire::PExpireReq;
use crate::protocol::key::unlink::{UnlinkParams, UnlinkReq};
use crate::protocol::set::spop::SPopReq;
use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType, next_snapshot_revision};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::{BaseOperation, InsertReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::OptionalU64;

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
        if let Err(error) = param.checked_deadline(update.write_clock) {
            return error.into();
        }
        self.execute_compute(param, update)
    }

    pub fn p_expire(&self, param: PExpireReq, update: &mut Update) -> Value {
        if let Err(error) = param.checked_deadline(update.write_clock) {
            return error.into();
        }
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

            UpdateType::Snapshot { queue, revision } => {
                let Some(entry) = cache.get(&del_req.key) else {
                    return Value::Integer(0);
                };
                if !cache.unlink(&del_req.key) {
                    return Value::Integer(0);
                }
                queue.push(AtomicRequest {
                    expected_revision: OptionalU64::some(entry.version),
                    version: next_snapshot_revision(revision),
                    request: BaseOperation::Unlink(del_req),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                Value::Integer(1)
            }
            UpdateType::CAS {
                expected_revision, ..
            } => {
                if let Some(entry) = cache.get(&del_req.key)
                    && expected_revision.get() == Some(entry.version)
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

            UpdateType::Snapshot { queue, revision } => {
                // remove returns only logically live entries, so missing or
                // expired keys neither affect the reply nor add a delta.
                let Some(entry) = cache.remove(&del_req.key) else {
                    return Value::Integer(0);
                };
                queue.push(AtomicRequest {
                    expected_revision: OptionalU64::some(entry.version),
                    version: next_snapshot_revision(revision),
                    request: BaseOperation::Del(del_req),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                Value::Integer(1)
            }
            UpdateType::CAS {
                expected_revision, ..
            } => {
                if let Some(entry) = cache.get(&del_req.key)
                    && expected_revision.get() == Some(entry.version)
                {
                    cache.remove(&del_req.key);
                    return Value::Integer(1);
                }
                Value::Integer(0)
            }
        }
    }

    pub fn flush_db(&self, req: FlushDBReq, update: &mut Update, external: bool) -> Value {
        let _lock = if external {
            Some(self.read_lock.write())
        } else {
            None
        };
        let cache = match self.get_cache(update.db_number) {
            Err(err) => return err,
            Ok(cache) => &cache.mocha,
        };
        match update.update_type {
            UpdateType::None => {
                cache.clear();
            }
            UpdateType::Snapshot { queue, revision } => {
                queue.push(AtomicRequest {
                    expected_revision: OptionalU64::NONE,
                    version: next_snapshot_revision(revision),
                    request: BaseOperation::FlushDB(req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                cache.clear();
            }
            UpdateType::CAS { revision, .. } => {
                // Full traversal may already contain writes made after this
                // FLUSH. Keep those entries while removing the older state.
                cache.retain_values(|value| value.version >= *revision);
            }
        }
        Value::ok()
    }

    pub fn flush_all(&self, req: FlushAllReq, update: &mut Update, external: bool) -> Value {
        let _lock = if external {
            Some(self.read_lock.write())
        } else {
            None
        };
        match update.update_type {
            UpdateType::None => {
                for database in &self.databases {
                    database.mocha.clear();
                }
            }
            UpdateType::Snapshot { queue, revision } => {
                queue.push(AtomicRequest {
                    expected_revision: OptionalU64::NONE,
                    version: next_snapshot_revision(revision),
                    request: BaseOperation::FlushAll(req.clone()),
                    write_clock: update.write_clock,
                    db_number: update.db_number,
                });
                for database in &self.databases {
                    database.mocha.clear();
                }
            }
            UpdateType::CAS { revision, .. } => {
                for database in &self.databases {
                    database
                        .mocha
                        .retain_values(|value| value.version >= *revision);
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
        param.execute(self, db_number, read_clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::ValueObject;
    use bytes::Bytes;

    #[test]
    fn snapshot_deletes_of_missing_or_expired_keys_do_not_enqueue() {
        for unlink in [false, true] {
            let cache = MyCache::new(1).unwrap();
            // Keep the expired entry physically present to exercise the command's
            // logical expiration check rather than a background worker deletion.
            cache.pause_expire_workers();
            cache.databases[0].mocha.insert_absolute(
                Bytes::from_static(b"expired"),
                MyValue::new(ValueObject::Int(10)),
                10,
            );
            cache.set_write_clock(10);
            let mut queue = Vec::new();
            let mut revision = 0;
            {
                let mut update_type = UpdateType::Snapshot {
                    queue: &mut queue,
                    revision: &mut revision,
                };
                let mut update = Update {
                    db_number: 0,
                    write_clock: 10,
                    update_type: &mut update_type,
                };
                for key in [b"missing".as_slice(), b"expired".as_slice()] {
                    let key = Bytes::copy_from_slice(key);
                    let reply = if unlink {
                        cache.unlink(UnlinkReq { key }, &mut update)
                    } else {
                        cache.del(DelReq { key }, &mut update)
                    };
                    assert_eq!(reply.encode(), b":0\r\n");
                }
            }
            assert!(queue.is_empty());
            assert_eq!(revision, 0);
            cache.resume_expire_workers();
        }
    }

    #[test]
    fn replayed_flush_preserves_newer_entries_and_database_scope() {
        for all in [false, true] {
            let cache = MyCache::new(2).unwrap();
            for database in &cache.databases {
                for (key, version) in [(b"normal".as_slice(), 0), (b"old", 3), (b"new", 9)] {
                    database.mocha.insert_persistent(
                        Bytes::copy_from_slice(key),
                        MyValue {
                            version,
                            data: ValueObject::Int(version as i64),
                        },
                    );
                }
            }
            let mut update_type = UpdateType::CAS {
                expected_revision: OptionalU64::NONE,
                revision: 7,
            };
            let mut update = Update {
                db_number: 1,
                write_clock: 0,
                update_type: &mut update_type,
            };
            let reply = if all {
                cache.flush_all(FlushAllReq { async_mode: false }, &mut update, true)
            } else {
                cache.flush_db(FlushDBReq { async_mode: false }, &mut update, true)
            };
            assert_eq!(reply.encode(), b"+OK\r\n");
            for (index, database) in cache.databases.iter().enumerate() {
                let cleared = all || index == 1;
                assert_eq!(database.mocha.get(b"normal".as_slice()).is_none(), cleared);
                assert_eq!(database.mocha.get(b"old".as_slice()).is_none(), cleared);
                assert_eq!(database.mocha.get(b"new".as_slice()).unwrap().version, 9);
            }
        }
    }
}
