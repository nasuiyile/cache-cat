use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::config::{TEMP_PATH, create_temp_dir, get_snapshot_file_name};
use crate::store::store::RaftMetaData;
use byteorder::LittleEndian;
use dashmap::DashMap;
use moka::Expiry;
use moka::future::Cache;
use moka::ops::compute::Op;
use openraft::SnapshotMeta;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem::size_of;
use std::option::Option;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex;
use tokio::{fs, io};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MyValue {
    #[serde(skip)]
    pub snapshot_num: u32, //快照编号 不需要进行序列化
    pub data: Arc<Vec<u8>>,
    pub ttl_ms: u64,
}

// =====================
// 内存估算相关常量
// =====================

const MY_VALUE_SIZE: usize = size_of::<MyValue>();
const ARC_COUNTER_SIZE: usize = 2 * size_of::<usize>(); // strong + weak
const VEC_SIZE: usize = size_of::<Vec<u8>>();

const CACHE_MAGIC_NUM: &[u8; 4] = b"MYC1";

const VERSION: u8 = 1;

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
    pub async fn insert(&self, key: Arc<Vec<u8>>, value: MyValue) {
        self.cache.insert(key, value).await
    }
    pub async fn snapshot_insert(
        &self,
        key: Arc<Vec<u8>>,
        value: MyValue,
        snapshot_num: u32,
        old_map: Arc<DashMap<Arc<Vec<u8>>, MyValue>>,
    ) {
        //发现存在老数据，放到old_map中
        self.cache
            .entry(key.clone())
            .or_insert_with_if(async { value }, |old_value| {
                if old_value.snapshot_num <= snapshot_num {
                    old_map.entry(key.clone()).or_insert(old_value.clone());
                }
                true
            })
            .await;
        return;
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

    pub async fn append(&self, key: Arc<Vec<u8>>, suffix: Arc<Vec<u8>>, snapshot_num: u32) {
        self.cache
            .entry(key)
            .and_compute_with(move |maybe_entry| {
                let suffix = suffix.clone();
                async move {
                    match maybe_entry {
                        Some(entry) => {
                            let mut value = entry.into_value();
                            // 关键：不会复制旧数据
                            let data = Arc::make_mut(&mut value.data);
                            data.extend_from_slice(&suffix);
                            Op::Put(value)
                        }
                        None => {
                            let mut v = Vec::with_capacity(suffix.len());
                            v.extend_from_slice(&suffix);
                            Op::Put(MyValue {
                                snapshot_num,
                                data: Arc::new(v),
                                ttl_ms: 0,
                            })
                        }
                    }
                }
            })
            .await;
    }

    // todo 优化为字节编码
    //流式序列化和反序列化
    pub async fn dump_cache_to_writer<W>(
        &self,
        writer: &mut W,
        old_map: Arc<DashMap<Arc<Vec<u8>>, MyValue>>,
        removed_map: Arc<DashMap<Arc<Vec<u8>>, MyValue>>,
        current_snapshot_num: u32,
    ) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        for entry in self.cache.iter() {
            let (k_arc, mut v) = entry;
            if v.snapshot_num >= current_snapshot_num {
                // 如果数据是新的
                if let Some(old_v) = old_map.get(&*k_arc) {
                    v = old_v.to_owned();
                } else {
                    continue;
                }
            }
            let key_bytes = bincode2::serialize(&*k_arc)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let val_bytes = bincode2::serialize(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
        }
        for entry in removed_map.iter() {
            let k_arc = entry.key();
            let v = entry.value();
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
            self.insert(Arc::new(key_vec), value).await;
        }

        Ok(())
    }
}
pub async fn dump_cache_to_path<P>(
    cache: MyCache,
    meta: SnapshotMeta<TypeConfig>,
    path: P,
    group_id: GroupId,
    old_map: Arc<DashMap<Arc<Vec<u8>>, MyValue>>,
    removed_map: Arc<DashMap<Arc<Vec<u8>>, MyValue>>,
    current_snapshot_num: u32,
    raft_meta: Arc<Mutex<RaftMetaData>>,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let snapshot_dir = path.join("snapshot");
    // 确保 snapshot 文件夹存在
    fs::create_dir_all(&snapshot_dir).await?;

    // 创建临时文件名
    let temp_filename = format!("snapshot_from_mem_{}_{}.tmp", Uuid::new_v4(), group_id);
    let final_filename = get_snapshot_file_name(group_id as GroupId);

    let temp_path = snapshot_dir.join(&temp_filename);
    let final_path = snapshot_dir.join(&final_filename);
    tracing::info!("dump cache to {}", final_path.display());
    // 写入临时文件
    let f = File::create(&temp_path).await?;
    // 通过 with_capacity 指定缓冲区大小 如果缓冲区满了则会自动 flush，让操作系统决定刷盘时间（flush不是真正刷盘，sync才是真正刷盘）
    let mut writer = BufWriter::with_capacity(1024 * 1024,f);

    writer.write_all(CACHE_MAGIC_NUM).await?;
    writer.write_u8(VERSION).await?;

    let result =
        bincode2::serialize(&meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writer.write_u64(result.len() as u64).await?;
    writer.write_all(&result).await?;

    cache
        .dump_cache_to_writer(&mut writer, old_map, removed_map, current_snapshot_num)
        .await?;
    //在最耗时的刷盘工作开始前将快照标记为已经结束 理论上可以在removed_map之前调用
    let mut raft_meta_data = raft_meta.lock().await;
    raft_meta_data.snapshot_state = false;
    drop(raft_meta_data);

    writer.flush().await?;
    writer.get_ref().sync_all().await?;

    // 通过 rename 原子替换目标文件
    fs::rename(&temp_path, &final_path).await?;

    Ok(())
}

pub async fn load_cache_from_path<P>(
    cache: MyCache,
    path: P,
) -> Result<Option<SnapshotMeta<TypeConfig>>, io::Error>
where
    P: AsRef<Path>,
{
    //先将缓存清空
    cache.invalidate_all();
    let path = path.as_ref();
    let f = match File::open(path).await {
        Ok(f) => f,
        //文件不存在
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(f);
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;
    if &magic != CACHE_MAGIC_NUM {
        return Err(io::Error::new(io::ErrorKind::Other, "invalid file magic"));
    }
    let mut version = [0u8; 1];
    reader.read_exact(&mut version).await?;
    if version[0] != VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "unsupported version"));
    }

    let meta_len = reader.read_u64().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: SnapshotMeta<TypeConfig> = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    cache.load_cache_from_reader(&mut reader).await?;
    Ok(Some(meta))
}

pub async fn load_meta_from_path<P>(path: P) -> Result<Option<SnapshotMeta<TypeConfig>>, io::Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();

    let f = match File::open(path).await {
        Ok(f) => f,
        //文件不存在
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(f);
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;
    if &magic != CACHE_MAGIC_NUM {
        return Err(io::Error::new(io::ErrorKind::Other, "invalid file magic"));
    }
    let mut version = [0u8; 1];
    reader.read_exact(&mut version).await?;
    if version[0] != VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "unsupported version"));
    }

    let meta_len = reader.read_u64().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: SnapshotMeta<TypeConfig> = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(meta))
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

#[tokio::test]
async fn test_dump_and_load_with_data() {
    let cache = MyCache::new();

    // 插入测试数据
    let key1 = Arc::new(b"key1".to_vec());
    let value1 = MyValue {
        snapshot_num: 0,
        data: Arc::new(b"value1".to_vec()),
        ttl_ms: 1000,
    };

    let key2 = Arc::new(b"key2".to_vec());
    let value2 = MyValue {
        snapshot_num: 0,
        data: Arc::new(b"value2".to_vec()),
        ttl_ms: 0, // 永不过期
    };

    cache.insert(key1.clone(), value1.clone()).await;
    cache.insert(key2.clone(), value2.clone()).await;

    let path = tempfile::Builder::new()
        .suffix("_1")
        .tempdir_in(TEMP_PATH)
        .unwrap()
        .keep()
        .join("");
    let meta: Arc<Mutex<RaftMetaData>> = Default::default();
    meta.lock().await.snapshot_num = 1;
    dump_cache_to_path(
        cache.clone(),
        Default::default(),
        path.clone(),
        1,
        Default::default(),
        Default::default(),
        1,
        meta,
    )
    .await
    .expect("dump cache should succeed");

    // 创建新缓存并加载数据
    let new_cache = MyCache::new();
    match load_cache_from_path(
        new_cache.clone(),
        path.join("snapshot").join(get_snapshot_file_name(1)),
    )
    .await
    {
        Ok(v) => println!("load ok: {:?}", v),
        Err(e) => {
            println!("load error: {:?}", e);
            return;
        }
    }

    // 验证数据完整性
    let loaded_value1 = new_cache.get(&key1).await;
    let loaded_value2 = new_cache.get(&key2).await;

    assert!(loaded_value1.is_some(), "key1 should exist");
    assert!(loaded_value2.is_some(), "key2 should exist");

    let v1 = loaded_value1.unwrap();
    let v2 = loaded_value2.unwrap();

    assert_eq!(v1.data.as_ref(), value1.data.as_ref());
    assert_eq!(v1.ttl_ms, value1.ttl_ms);
    println!("{:?}", v2.data);
    println!("{:?}", value2.data);
    assert_eq!(v2.data.as_ref(), value2.data.as_ref());
    assert_eq!(v2.ttl_ms, value2.ttl_ms);
}
