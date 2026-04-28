use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::now_ms;
use moka::Expiry;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::option::Option;
use std::sync::Arc;
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

// =====================
// 自定义 Expiry
// =====================

struct MyExpiry;

impl Expiry<Arc<Vec<u8>>, MyValue> for MyExpiry {
    //创建或更新后的定时删除逻辑
    fn expire_after_create(
        &self,
        _key: &Arc<Vec<u8>>,
        value: &MyValue,
        _created_at: Instant,
    ) -> Option<Duration> {
        if value.expires_at == 0 {
            None
        } else {
            let now = now_ms();
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
            let now = now_ms();
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
    pub batch_lock: Arc<Mutex<()>>,
}

impl MyCache {
    /// 创建 MyCache 时自动初始化内部 Cache
    pub fn new() -> Self {
        let cache = Cache::builder()
            // .max_capacity(max_capacity)
            .expire_after(MyExpiry)
            .build();
        Self {
            cache,
            batch_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 获取值
    pub fn count(&self) -> u64 {
        self.cache.entry_count()
    }
    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
}
pub enum UpdateType<'a> {
    None,
    Snapshot(&'a mut Vec<AtomicRequest>),
    CAS(u32),
}
