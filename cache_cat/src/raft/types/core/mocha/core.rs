use crate::error::ProtocolError;
use crate::mocha::Mocha;
use crate::protocol::common::lua_env::LuaEnv;
use crate::protocol::list::blpop::BlPopParams;
use crate::protocol::stream::xread::XReadParams;
use crate::protocol::stream::xreadgroup::XReadGroupParams;
use crate::raft::application::blocking_keys::{
    BlockingKeys, CleanupReceiver, ExclusiveRegistration, Registration, WaitError, cleanup_channel,
};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::{OptionalU64, now_ms};
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashSet;
use std::option::Option;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    /// Snapshot-only revision. Ordinary writes use 0; Start writes get unique revisions.
    pub version: u64,
    pub data: ValueObject,
}

impl MyValue {
    pub fn new(value: ValueObject) -> Self {
        Self {
            version: 0,
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
    pub write_lock: Arc<Mutex<WriteState>>, //单key写
    pub read_lock: Arc<RwLock<()>>,         //单key读

    read_logic_clock: Arc<AtomicU64>,  //读逻辑时钟
    write_logic_clock: Arc<AtomicU64>, //写逻辑时钟
}

/// Mutable node-local waiter state, protected by the existing global write
/// lock. Connection tasks and serial apply use the same guard; the registries
/// contain no additional mutex or atomic waiter count.
#[derive(Debug)]
pub struct WriteState {
    pub blocking_keys: BlockingKeys<WriteState, (u16, Bytes), Value, XReadParams>,
    pub list_blocking_keys: BlockingKeys<WriteState, (u16, Bytes), (), BlPopParams>,
    pub group_blocking_keys: BlockingKeys<WriteState, (u16, Bytes), (), XReadGroupParams>,
    cleanup_receiver: Option<CleanupReceiver<WriteState>>,
}

impl WriteState {
    /// All registration entry points use the same guarded state. Starting the
    /// one cleanup task is lazy, so creating MyCache needs no async runtime.
    fn prepare_registration(&mut self) -> Result<(), WaitError> {
        self.blocking_keys.start_cleanup(&mut self.cleanup_receiver)
    }

    pub(crate) fn register_stream(
        &mut self,
        keys: Vec<(u16, Bytes)>,
        deadline: Option<tokio::time::Instant>,
        params: XReadParams,
    ) -> Result<Registration<Self, (u16, Bytes), Value, XReadParams>, WaitError> {
        if keys.is_empty() {
            return Err(WaitError::EmptyKeys);
        }
        self.prepare_registration()?;
        self.blocking_keys.register_with(keys, deadline, params)
    }

    pub(crate) fn register_stream_group(
        &mut self,
        keys: Vec<(u16, Bytes)>,
        deadline: Option<tokio::time::Instant>,
        params: XReadGroupParams,
    ) -> Result<ExclusiveRegistration<Self, (u16, Bytes), XReadGroupParams>, WaitError> {
        if keys.is_empty() {
            return Err(WaitError::EmptyKeys);
        }
        self.prepare_registration()?;
        self.group_blocking_keys
            .register_exclusive_with(keys, deadline, params)
    }

    pub(crate) fn register_list(
        &mut self,
        keys: Vec<(u16, Bytes)>,
        deadline: Option<tokio::time::Instant>,
        params: BlPopParams,
    ) -> Result<ExclusiveRegistration<Self, (u16, Bytes), BlPopParams>, WaitError> {
        if keys.is_empty() {
            return Err(WaitError::EmptyKeys);
        }
        self.prepare_registration()?;
        self.list_blocking_keys
            .register_exclusive_with(keys, deadline, params)
    }
}

/// Allocate through the exclusive borrow held by the state machine's existing
/// metadata guard. The high watermark survives batches and snapshot failures.
pub(crate) fn next_snapshot_revision(revision: &mut u64) -> u64 {
    assert!(*revision < u64::MAX - 1, "snapshot revision exhausted");
    *revision += 1;
    *revision
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

    pub fn reset_write_clock(&self) {
        self.write_logic_clock.store(0, Ordering::Release);
        self.read_logic_clock.store(0, Ordering::Release);
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
        // Creating the cache remains synchronous and requires no Tokio
        // runtime. The first blocking registration starts cleanup lazily.
        let (cleanup_sender, cleanup_receiver) = cleanup_channel();
        let write_lock = Arc::new_cyclic(|owner| {
            Mutex::new(WriteState {
                blocking_keys: BlockingKeys::new(
                    owner.clone(),
                    cleanup_sender.clone(),
                    |state| &mut *state.blocking_keys,
                    |state, id, _retry| state.blocking_keys.cancel(id),
                ),
                list_blocking_keys: BlockingKeys::new(
                    owner.clone(),
                    cleanup_sender.clone(),
                    |state| &mut *state.list_blocking_keys,
                    |state, id, retry| state.list_blocking_keys.release_exclusive(id, retry),
                ),
                group_blocking_keys: BlockingKeys::new(
                    owner.clone(),
                    cleanup_sender,
                    |state| &mut *state.group_blocking_keys,
                    |state, id, retry| state.group_blocking_keys.release_exclusive(id, retry),
                ),
                cleanup_receiver: Some(cleanup_receiver),
            })
        });
        Ok(Self {
            lua_env,
            read_logic_clock: Arc::new(AtomicU64::new(0)),
            write_logic_clock,
            databases: vec,
            write_lock,
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

    pub fn pause_expire_workers(&self) {
        for db in &self.databases {
            db.mocha.pause_expire_worker_blocking();
        }
    }

    pub fn resume_expire_workers(&self) {
        for db in &self.databases {
            db.mocha.resume_expire_worker_blocking();
        }
    }
}

/// Leader-local notification effects collected by the serial apply task.
/// Reuse allocation within a batch, but drain after each outer command so
/// EXEC/Lua publish only their final state. Empty sets allocate no buckets.
#[derive(Debug, Default)]
pub struct ApplyEffects {
    pub(crate) ready_streams: HashSet<(u16, Bytes)>,
    pub(crate) ready_lists: HashSet<(u16, Bytes)>,
    pub(crate) ready_groups: HashSet<(u16, Bytes)>,
}

impl ApplyEffects {
    /// Reuse small tables without retaining a large transaction's buckets.
    /// Called only after a nonempty notification set has been processed.
    pub(crate) fn recycle_ready_keys(keys: &mut HashSet<(u16, Bytes)>) {
        debug_assert!(keys.is_empty());
        if keys.capacity() > 64 {
            *keys = HashSet::new();
        }
    }
}

pub struct Update<'a> {
    /// The applied Raft log index and the XGROUP CREATE ordinal within it.
    /// This deterministic identity is copied into newly created group metadata.
    /// Snapshot replay keeps the token already stored in its concrete operation.
    pub execution_id: Option<(u64, u64)>,
    pub db_number: u16,
    pub write_clock: u64,
    pub update_type: &'a mut UpdateType<'a>,
    /// Borrowed only by serial command execution; snapshot replay uses None.
    /// The owner already holds write_lock, so these effects need no own lock.
    pub effects: Option<&'a mut ApplyEffects>,
    /// Borrowed from the existing write guard; absent during snapshot replay.
    pub waiters: Option<&'a mut WriteState>,
}

pub enum UpdateType<'a> {
    //正常运行
    None,
    //进行快照
    Snapshot {
        queue: &'a mut Vec<AtomicRequest>,
        revision: &'a mut u64,
    },
    //还原操作
    CAS {
        expected_revision: OptionalU64,
        revision: u64,
    },
}
