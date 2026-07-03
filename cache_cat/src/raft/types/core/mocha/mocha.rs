use crate::error::ProtocolError;
use crate::mocha::Mocha;
use crate::protocol::lua_env::LuaEnv;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::now_ms;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::option::Option;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    pub version: u32, //在快照期间每一次更新都会增加version 默认为1
    pub data: ValueObject,
}

impl MyValue {
    pub fn new(value: ValueObject) -> Self {
        Self {
            version: 1,
            data: value,
        }
    }
}

#[derive(Debug)]
pub struct Database {
    // pub cache: Cache<Arc<Vec<u8>>, MyValue>,
    pub mocha: Mocha<Bytes, MyValue>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            mocha: self.mocha.clone(),
        }
    }
}

#[derive(Debug)]
pub struct MyCache {
    pub lua_env: LuaEnv,

    pub databases: Vec<Database>,
    // 这俩把锁是为了保证每条指令的原子性 多key写，多key读需要同时获取俩把锁 同时获取俩把锁时 先加write_lock
    pub write_lock: Arc<Mutex<()>>, //单key写
    pub read_lock: Arc<RwLock<()>>, //单key读

    read_logic_clock: Arc<AtomicU64>,  //读逻辑时钟
    write_logic_clock: Arc<AtomicU64>, //写逻辑时钟
}

impl MyCache {
    pub fn get_cache(&self, db_number: u16) -> Result<&Database, Value> {
        self.databases
            .get(db_number as usize)
            .ok_or(ProtocolError::DbNotExist.into())
    }

    #[inline]
    pub fn get_and_update_read_clock(&self) -> u64 {
        let write_time = self.write_logic_clock.load(Ordering::Acquire);
        let system_now = now_ms();
        let target = max(write_time, system_now);
        let old_val = self.read_logic_clock.fetch_max(target, Ordering::Release);
        // 最终的逻辑时钟值应该是 target 和旧值中的最大者
        max(old_val, target)
    }

    #[inline]
    pub fn generate_new_write_clock(&self) -> u64 {
        let read_time = self.read_logic_clock.load(Ordering::Acquire);
        let system_now = now_ms();

        max(read_time, system_now)
    }

    pub fn set_write_clock(&self, new_clock: u64) -> u64 {
        let old_value = self
            .write_logic_clock
            .fetch_max(new_clock, Ordering::Release);
        max(old_value, new_clock)
    }

    pub fn get_write_clock(&self) -> u64 {
        self.write_logic_clock.load(Ordering::Acquire)
    }

    /// 创建 MyCache 时自动初始化内部 Cache
    pub fn new(db_number: u16) -> Result<Self, ProtocolError> {
        let write_logic_clock = Arc::new(AtomicU64::new(0));
        let mut vec = Vec::new();
        for _ in 0..db_number {
            let mocha = Mocha::new(write_logic_clock.clone());
            let db = Database { mocha };
            vec.push(db);
        }
        let lua_env = LuaEnv::new()?;
        Ok(Self {
            lua_env,
            read_logic_clock: Arc::new(AtomicU64::new(0)),
            write_logic_clock,
            databases: vec,
            write_lock: Arc::new(Default::default()),
            read_lock: Arc::new(Default::default()),
        })
    }

    #[inline]
    pub fn get_value_with_read_clock(
        &self,
        key: &[u8],
        db_number: u16,
    ) -> Result<Option<MyValue>, ProtocolError> {
        let cache = self
            .databases
            .get(db_number as usize)
            .ok_or(ProtocolError::DbNotExist)?;
        let read_clock = self.get_and_update_read_clock();
        match cache.mocha.get_entry(key) {
            None => {
                //用写逻辑时钟也获取不到 可能会产生写逻辑时钟在此刻推进了导致读不到数据。但这是符合预期的。
                Ok(None)
            }
            Some(my_value) => {
                match my_value.expire_at {
                    Some(inner) => {
                        if inner < read_clock {
                            // 写逻辑时钟获取到了但是读逻辑时钟没有获取到
                            return Ok(None);
                        }
                        Ok(Some(my_value.value))
                    }
                    None => Ok(Some(my_value.value)),
                }
            }
        }
    }

    #[inline]
    pub fn get_values_with_read_clock(
        &self,
        keys: &[&[u8]],
        db_number: u16,
    ) -> Result<Vec<Option<MyValue>>, ProtocolError> {
        let cache = self
            .databases
            .get(db_number as usize)
            .ok_or(ProtocolError::DbNotExist)?;
        let read_clock = self.get_and_update_read_clock();
        keys.iter()
            .map(|&key| {
                match cache.mocha.get_entry(key) {
                    None => {
                        // 用写逻辑时钟也获取不到，可能会产生写逻辑时钟在此刻推进了导致读不到数据。
                        // 但这是符合预期的。
                        Ok(None)
                    }
                    Some(my_value) => {
                        match my_value.expire_at {
                            Some(inner) => {
                                if inner < read_clock {
                                    // 写逻辑时钟获取到了但是读逻辑时钟没有获取到
                                    return Ok(None);
                                }
                                Ok(Some(my_value.value))
                            }
                            None => Ok(Some(my_value.value)),
                        }
                    }
                }
            })
            .collect()
    }

    pub fn invalidate_all(&self) {
        for db in &self.databases {
            db.mocha.clear();
        }
    }
}

pub struct Update<'a> {
    pub db_number: u16,
    pub write_clock: u64,
    pub update_type: &'a mut UpdateType<'a>,
}

pub enum UpdateType<'a> {
    None,
    Snapshot(&'a mut Vec<AtomicRequest>),
    CAS(u32),
}
