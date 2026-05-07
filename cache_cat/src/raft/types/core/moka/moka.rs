use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::now_ms;
use moka::Expiry;
use moka::sync::Cache;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::option::Option;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    pub version: u32, //在快照期间每一次更新都会增加version 默认为1
    pub data: ValueObject,
    pub expires_at: u64, //绝对时间  这里 假设不同节点的时钟偏移是有界的
}

impl MyValue {
    pub fn estimated_memory_usage(&self) -> usize {
        // MY_VALUE_SIZE + ARC_COUNTER_SIZE + VEC_SIZE + self.data.capacity()
        0
    }
}

// 自定义 Expiry
struct MyExpiry {
    write_logic_clock: Arc<AtomicU64>, //写逻辑时钟的副本
}
impl Expiry<Arc<Vec<u8>>, MyValue> for MyExpiry {
    //创建或更新后的定时删除逻辑
    //唯一的过期方式是推动写逻辑时钟的推进
    fn expire_after_create(
        &self,
        _key: &Arc<Vec<u8>>,
        value: &MyValue,
        _created_at: Instant,
    ) -> Option<Duration> {
        if value.expires_at == 0 {
            None
        } else {
            let now = self.write_logic_clock.load(Ordering::Acquire);
            if value.expires_at <= now {
                Some(Duration::from_millis(0))
            } else {
                Some(Duration::from_millis(value.expires_at - now))
            }
        }
    }

    fn expire_after_update(
        &self,
        _key: &Arc<Vec<u8>>,
        value: &MyValue,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        if value.expires_at == 0 {
            None
        } else {
            let now = self.write_logic_clock.load(Ordering::Acquire);
            if value.expires_at <= now {
                Some(Duration::from_millis(0))
            } else {
                Some(Duration::from_millis(value.expires_at - now))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyCache {
    // 内部 Cache的Clone成本是低廉的
    pub cache: Cache<Arc<Vec<u8>>, MyValue>,
    // 这俩把锁是为了保证每条指令的原子性 多key写，多key读需要同时获取俩把锁 同时获取俩把锁时 先加write_lock
    pub write_lock: Arc<Mutex<()>>, //单key写
    pub read_lock: Arc<Mutex<()>>,  //单key读

    read_logic_clock: Arc<AtomicU64>,  //读逻辑时钟
    write_logic_clock: Arc<AtomicU64>, //写逻辑时钟
}

impl MyCache {
    pub fn get_and_update_read_clock(&self) -> u64 {
        let write_time = self.write_logic_clock.load(Ordering::Acquire);
        let system_now = now_ms();

        // 取 write 和 system 的较大者作为更新目标
        let target = max(write_time, system_now);

        // 使用 fetch_max 自动完成：
        // 如果 target > current，则更新并返回旧值；否则不更新。
        // 注意：fetch_max 返回的是旧值 (previous value)
        let old_val = self.read_logic_clock.fetch_max(target, Ordering::Release);

        // 最终的逻辑时钟值应该是 target 和旧值中的最大者
        max(old_val, target)
    }
    pub fn get_new_write_clock(&self) -> u64 {
        let read_time = self.read_logic_clock.load(Ordering::Acquire);
        let system_now = now_ms();
        let target = max(read_time, system_now);
        target
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
    pub fn new() -> Self {
        let write_logic_clock = Arc::new(AtomicU64::new(0));
        let cache = Cache::builder()
            // .max_capacity(max_capacity)
            .expire_after(MyExpiry {
                write_logic_clock: write_logic_clock.clone(),
            })
            .build();
        Self {
            cache,
            write_lock: Arc::new(Mutex::new(())),
            read_lock: Arc::new(Mutex::new(())),
            read_logic_clock: Arc::new(AtomicU64::new(0)),
            write_logic_clock,
        }
    }

    pub fn get_value_with_read_clock(&self, key: &Vec<u8>) -> Option<MyValue> {
        let read_clock = self.get_and_update_read_clock();
        match self.cache.get(key) {
            None => {
                //用写逻辑时钟也获取不到 可能会产生写逻辑时钟在此刻推进了导致读不到数据。但这是符合预期的。
                None
            }
            Some(my_value) => {
                if my_value.expires_at < read_clock && my_value.expires_at != 0 {
                    //写逻辑时钟获取到了 但是读逻辑时钟没有获取到
                    return None;
                }
                Some(my_value)
            }
        }
    }
}
pub enum UpdateType<'a> {
    None,
    Snapshot(&'a mut Vec<AtomicRequest>, u64),
    CAS(u32),
}
