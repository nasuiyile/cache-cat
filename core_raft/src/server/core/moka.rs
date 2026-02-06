use moka::Expiry;
use moka::sync::Cache;
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct MyValue {
    pub data: Arc<Vec<u8>>,
    pub ttl_ms: u64,
}

// =====================
// 内存估算相关常量
// =====================

const MY_VALUE_SIZE: usize = size_of::<MyValue>();
const ARC_COUNTER_SIZE: usize = 2 * size_of::<usize>(); // strong + weak
const VEC_SIZE: usize = size_of::<Vec<u8>>();

impl MyValue {
    pub fn estimated_memory_usage(&self) -> usize {
        MY_VALUE_SIZE + ARC_COUNTER_SIZE + VEC_SIZE + self.data.capacity()
    }
}

// =====================
// 自定义 Expiry
// =====================

struct MyExpiry;

impl Expiry<String, MyValue> for MyExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &MyValue,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(Duration::from_millis(value.ttl_ms))
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &MyValue,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(Duration::from_millis(value.ttl_ms))
    }
}

// =====================
// Cache 所在的结构体
// =====================

pub struct MyCache {
    cache: Cache<String, MyValue>,
}

impl MyCache {
    /// 创建 MyCache 时自动初始化内部 Cache
    pub fn new(max_capacity: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .expire_after(MyExpiry)
            .build();

        Self { cache }
    }

    /// 插入值
    pub fn insert(&self, key: String, value: MyValue) {
        self.cache.insert(key, value);
    }

    /// 获取值
    pub fn get(&self, key: &str) -> Option<MyValue> {
        self.cache.get(key)
    }
}
