use crate::network::model::{AtomicRequest, Request, Value};
use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::config::{TEMP_PATH, create_temp_dir, get_snapshot_file_name};
use crate::server::handler::model::{LPushReq, SetReq};
use crate::store::store::RaftMetaData;
use byteorder::LittleEndian;
use dashmap::DashMap;
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::{CompResult, Op};
use openraft::SnapshotMeta;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList};
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
    pub data: ValueObject,
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
            data: ValueObject::String(set_req.value.clone()),
            ttl_ms: set_req.ex_time,
            version: 0,
        };
        self.cache.insert(set_req.key, value).await;
    }
    pub async fn snapshot_set(&self, set_req: SetReq, queue: &mut Vec<AtomicRequest>) {
        let mut value = MyValue {
            data: ValueObject::String(set_req.value.clone()),
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
        let new_data = ValueObject::String(set_req.value.clone());
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
    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
    pub async fn l_push(&self, l_push: LPushReq) -> Value {
        let result = self
            .cache
            .entry(l_push.key)
            .and_compute_with(|maybe_entry| async move {
                match maybe_entry {
                    Some(entry) => {
                        let mut value = entry.into_value();
                        match &mut value.data {
                            ValueObject::List(data) => {
                                value.version += 1;
                                data.push_front(l_push.value);
                                Op::Put(value)
                            }
                            _ => Op::Nop,
                        }
                    }
                    None => {
                        let value = MyValue {
                            data: ValueObject::List(LinkedList::from([l_push.value])),
                            ttl_ms: 0,
                            version: 0,
                        };
                        Op::Put(value)
                    }
                }
            })
            .await;
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::List(data_arc) => Value::Integer(data_arc.len() as i64),
                _ => Value::Error("Key exists but is not a List".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Value::Error("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }

    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
    pub async fn l_push_snapshot(
        &self,
        l_push: LPushReq,
        queue: &mut Vec<AtomicRequest>,
    ) -> Value {
        let result = self
            .cache
            .entry(l_push.key.clone())
            .and_compute_with(|maybe_entry| async move {
                match maybe_entry {
                    Some(entry) => {
                        let mut value = entry.into_value();
                        match &mut value.data {
                            ValueObject::List(data) => {
                                queue.push(AtomicRequest {
                                    version: value.version,
                                    request: Request::LPush(l_push.clone()),
                                });
                                value.version += 1;
                                data.push_front(l_push.value);
                                Op::Put(value)
                            }
                            _ => Op::Nop,
                        }
                    }
                    None => {
                        queue.push(AtomicRequest {
                            version: 1,
                            request: Request::LPush(l_push.clone()),
                        });
                        let value = MyValue {
                            data: ValueObject::List(LinkedList::from([l_push.value])),
                            ttl_ms: 0,
                            version: 1,
                        };
                        Op::Put(value)
                    }
                }
            })
            .await;
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::List(data_arc) => Value::Integer(data_arc.len() as i64),
                _ => Value::Error("Key exists but is not a List".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Value::Error("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }

    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
    pub async fn l_push_cas(&self, l_push: LPushReq, version: u32) -> Value {
        let result = self
            .cache
            .entry(l_push.key.clone())
            .and_compute_with(|maybe_entry| async move {
                match maybe_entry {
                    Some(entry) => {
                        let mut value = entry.into_value();
                        match &mut value.data {
                            ValueObject::List(data) => {
                                if value.version != version {
                                    return Op::Nop;
                                }
                                value.version += 1;
                                data.push_front(l_push.value);
                                Op::Put(value)
                            }
                            _ => Op::Nop,
                        }
                    }
                    None => {
                        if version != 0 {
                            //理论上不会出现
                            tracing::error!("CAS failed: operation not found");
                        }
                        let value = MyValue {
                            data: ValueObject::List(LinkedList::from([l_push.value])),
                            ttl_ms: 0,
                            version: 1,
                        };
                        Op::Put(value)
                    }
                }
            })
            .await;
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::List(data_arc) => Value::Integer(data_arc.len() as i64),
                _ => Value::Error("Key exists but is not a List".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Value::Error("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }

    //如果不是string就报错，如果是string就append，如果没有值就创建一个
    pub async fn append(&self, key: Arc<Vec<u8>>, suffix: Arc<Vec<u8>>) -> Result<u32, String> {
        let result = self
            .cache
            .entry(key)
            .and_compute_with(|maybe_entry| {
                let suffix = suffix.clone();
                async move {
                    match maybe_entry {
                        Some(entry) => {
                            let mut value = entry.into_value();
                            match &mut value.data {
                                ValueObject::String(data_arc) => {
                                    let data = Arc::make_mut(data_arc);
                                    data.extend_from_slice(&suffix);
                                    value.version += 1;
                                    Op::Put(value)
                                }
                                _ => {
                                    // 这里不能返回 Err，只能 Nop 或 Put
                                    Op::Nop
                                }
                            }
                        }
                        None => Op::Put(MyValue {
                            data: ValueObject::String(suffix.clone()),
                            ttl_ms: 0,
                            version: 1,
                        }),
                    }
                }
            })
            .await;

        //  在这里统一解析返回值
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::String(data_arc) => Ok(data_arc.len() as u32),
                _ => Err("Key exists but is not a String".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Err("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Err("Unexpected: value removed".to_string()),
        }
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
pub enum ValueObject {
    Int(i32),

    String(Arc<Vec<u8>>),
    List(LinkedList<Arc<Vec<u8>>>),

    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
}
