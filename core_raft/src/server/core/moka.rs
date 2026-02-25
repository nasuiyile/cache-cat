use moka::Expiry;
use moka::sync::Cache;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Serialize, Deserialize)]
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

impl Expiry<Arc<Vec<u8>>, MyValue> for MyExpiry {
    //创建或更新后的定时删除逻辑
    fn expire_after_create(
        &self,
        _key: &Arc<Vec<u8>>,
        value: &MyValue,
        _created_at: Instant,
    ) -> Option<Duration> {
        if value.ttl_ms == 0 {
            None // 永不过期
        } else {
            Some(Duration::from_millis(value.ttl_ms))
        }
    }

    fn expire_after_update(
        &self,
        _key: &Arc<Vec<u8>>,
        value: &MyValue,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        if value.ttl_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(value.ttl_ms))
        }
    }
}

#[derive(Debug, Clone)]
pub struct MyCache {
    // 内部 Cache的Clone成本是低廉的
    cache: Cache<Arc<Vec<u8>>, MyValue>,
}

impl MyCache {
    /// 创建 MyCache 时自动初始化内部 Cache
    pub fn new() -> Self {
        let cache = Cache::builder()
            // .max_capacity(max_capacity)
            .expire_after(MyExpiry)
            .build();
        Self { cache }
    }

    /// 插入值
    pub fn insert(&self, key: Arc<Vec<u8>>, value: MyValue) {
        self.cache.insert(key, value);
    }

    /// 获取值
    pub fn get(&self, key: &Arc<Vec<u8>>) -> Option<MyValue> {
        self.cache.get(key)
    }
}
impl Serialize for MyCache {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let entries: Vec<(Vec<u8>, MyValue)> = self
            .cache
            .iter()
            .map(|(k, v)| ((**k).clone(), v.clone()))
            .collect();

        entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MyCache {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries: Vec<(Vec<u8>, MyValue)> = Vec::deserialize(deserializer)?;

        let cache = MyCache::new();

        for (k, v) in entries {
            cache.insert(Arc::new(k), v);
        }

        Ok(cache)
    }
}
