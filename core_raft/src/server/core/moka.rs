use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList};
use crate::network::model::{AtomicRequest, Request};
use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::config::{TEMP_PATH, create_temp_dir, get_snapshot_file_name};
use crate::server::handler::model::SetReq;
use crate::store::store::RaftMetaData;
use byteorder::LittleEndian;
use dashmap::DashMap;
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::Op;
use openraft::SnapshotMeta;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::io::SeekFrom;
use std::mem::size_of;
use std::option::Option;
use std::os::raw::c_int;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::sync::Mutex;
use tokio::{fs, io};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    pub version: u32, //在快照期间每一次更新都会增加version 默认为1
    pub data: Value,
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
    pub cache: Cache<Arc<Vec<u8>>, MyValue>,
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
    pub async fn set(&self, set_req: SetReq) {
        let value = MyValue {
            data: Value::String(set_req.value.clone()),
            ttl_ms: set_req.ex_time,
            version: 0,
        };
        self.cache.insert(set_req.key, value).await;
    }
    pub async fn snapshot_set(&self, set_req: SetReq, queue: &mut Vec<AtomicRequest>) {
        let mut value = MyValue {
            data: Value::String(set_req.value.clone()),
            ttl_ms: set_req.ex_time,
            version: 0,
        };
        //发现存在老数据，获取老数据版本
        self.cache
            .entry(set_req.key.clone())
            .and_upsert_with(|old_entry| async move {
                let old_version = if let Some(entry) = old_entry {
                    entry.into_value().version + 1
                } else {
                    //不存在默认为0
                    0
                };
                value.version = old_version;
                queue.push(AtomicRequest {
                    version: value.version,
                    request: Request::Set(set_req),
                });
                value
            })
            .await;
        return;
    }
    pub async fn cas_set(&self, set_req: SetReq, version: u32) {
        let key = set_req.key.clone();
        // 我们预先准备好要插入的新数据
        let new_data = Value::String(set_req.value.clone());
        let ttl = set_req.ex_time;
        self.cache
            .entry(key)
            .and_upsert_with(async move |maybe_entry| {
                if let Some(entry) = maybe_entry {
                    let current_val = entry.value();
                    // 核心逻辑：只有传入的 version 与缓存中的 version 相同时才允许更新
                    if version == current_val.version {
                        MyValue {
                            data: new_data,
                            ttl_ms: ttl,
                            version: current_val.version + 1, // 更新版本号
                        }
                    } else {
                        // 版本不匹配，直接返回旧值（即不更新）
                        current_val.clone()
                    }
                } else {
                    // 如果缓存里根本没数据：
                    // 此时你可以根据业务决定：是允许插入（version从0开始），还是报错
                    MyValue {
                        data: new_data,
                        ttl_ms: ttl,
                        version: 1, // 初始版本
                    }
                }
            })
            .await;
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    /// 获取值
    pub async fn get(&self, key: &Arc<Vec<u8>>) -> Option<MyValue> {
        self.cache.get(key).await
    }
    pub fn count(&self) -> u64 {
        self.cache.entry_count()
    }

    pub async fn append(&self, key: Arc<Vec<u8>>, suffix: Arc<Vec<u8>>) {
        self.cache
            .entry(key)
            .and_compute_with(move |maybe_entry| {
                let suffix = suffix.clone();
                async move {
                    match maybe_entry {
                        Some(entry) => {
                            let mut value = entry.into_value();
                            match &mut value.data {
                                Value::String(data_arc) => {
                                    // 关键：只在 String 类型下 append
                                    let data = Arc::make_mut(data_arc);
                                    data.extend_from_slice(&suffix);
                                    value.version += 1;
                                    Op::Put(value)
                                }
                                // 方案1：保持原值（最安全）
                                _ => Op::Put(value),
                            }
                        }
                        // key 不存在 → 创建 String
                        None => {
                            Op::Put(MyValue {
                                data: Value::String(suffix.clone()),
                                ttl_ms: 0,
                                version: 1,
                            })
                        }
                    }
                }
            })
            .await;
    }

    // todo 优化为字节编码
    //流式序列化和反序列化
    pub async fn dump_cache_to_writer<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        for entry in self.cache.iter() {
            let (k_arc, v) = entry;
            let key_bytes = bincode2::serialize(&*k_arc)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let val_bytes = bincode2::serialize(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
        }

        writer.write_u64(0).await?;
        Ok(())
    }
    pub async fn load_cache_from_reader<R>(&self, reader: &mut R) -> Result<(), io::Error>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            let key_len = match reader.read_u64().await {
                Ok(v) => v as usize,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };
            if key_len == 0 {
                break;
            }

            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf).await?;

            let val_len = reader.read_u64().await? as usize;
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf).await?;

            let key_vec: Vec<u8> = bincode2::deserialize(&key_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: MyValue = bincode2::deserialize(&val_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.cache.insert(Arc::new(key_vec), value).await;
        }

        Ok(())
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value{
    Int(i32),

    String(Arc<Vec<u8>>),
    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    List(LinkedList<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
}