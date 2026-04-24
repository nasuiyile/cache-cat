use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, LPushReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::{now_ms, parse_i64};
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap, HashSet, LinkedList};
use std::mem::size_of;
use std::option::Option;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    pub version: u32, //在快照期间每一次更新都会增加version 默认为1
    pub data: ValueObject,
    pub expires_at: u64, //绝对时间  这里 假设不同节点的时钟偏移是有界的
}

// =====================
// 内存估算相关常量
// =====================

const MY_VALUE_SIZE: usize = size_of::<MyValue>();
const ARC_COUNTER_SIZE: usize = 2 * size_of::<usize>(); // strong + weak
const VEC_SIZE: usize = size_of::<Vec<u8>>();

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
}
pub enum UpdateType<'a> {
    None,
    Snapshot(&'a mut Vec<AtomicRequest>),
    CAS(u32),
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


    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 获取值
    pub fn count(&self) -> u64 {
        self.cache.entry_count()
    }
    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list

}
