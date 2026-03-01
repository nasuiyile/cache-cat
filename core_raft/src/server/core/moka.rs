use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::config::{create_temp_dir, get_snapshot_file_name};
use byteorder::LittleEndian;
use moka::Expiry;
use moka::sync::Cache;
use openraft::SnapshotMeta;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use std::mem::size_of;
use std::option::Option;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::{fs, io};
use tracing::trace;
use uuid::Uuid;

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
    pub fn insert(&self, key: Arc<Vec<u8>>, value: MyValue) {
        self.cache.insert(key, value);
    }

    /// 获取值
    pub fn get(&self, key: &Arc<Vec<u8>>) -> Option<MyValue> {
        self.cache.get(key)
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
            self.insert(Arc::new(key_vec), value);
        }

        Ok(())
    }
}
pub async fn dump_cache_to_path<P>(
    cache: MyCache,
    meta: SnapshotMeta<TypeConfig>,
    path: P,
) -> Result<(), std::io::Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let snapshot_dir = path.join("snapshot");
    // 确保 snapshot 文件夹存在
    fs::create_dir_all(&snapshot_dir).await?;

    // 创建临时文件名
    let group_id = 0; // 这里应该传入实际的 group_id，可能需要修改函数参数
    let temp_filename = format!("snapshot_from_mem_{}_{}.tmp", Uuid::new_v4(), group_id);
    let final_filename = get_snapshot_file_name(group_id as GroupId);

    let temp_path = snapshot_dir.join(&temp_filename);
    let final_path = snapshot_dir.join(&final_filename);
    tracing::info!("dump cache to {}", final_path.display());
    // 写入临时文件
    let f = File::create(&temp_path).await?;
    let mut writer = BufWriter::new(f);

    writer.write_all(CACHE_MAGIC_NUM).await?;
    writer.write_u8(VERSION).await?;

    let result =
        bincode2::serialize(&meta).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writer.write_u64(result.len() as u64).await?;
    writer.write_all(&result).await?;

    cache.dump_cache_to_writer(&mut writer).await?;
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
#[tokio::test]
async fn test_dump_and_load_with_data() {
    let cache = MyCache::new();

    // 插入测试数据
    let key1 = Arc::new(b"key1".to_vec());
    let value1 = MyValue {
        data: Arc::new(b"value1".to_vec()),
        ttl_ms: 1000,
    };

    let key2 = Arc::new(b"key2".to_vec());
    let value2 = MyValue {
        data: Arc::new(b"value2".to_vec()),
        ttl_ms: 0, // 永不过期
    };

    cache.insert(key1.clone(), value1.clone());
    cache.insert(key2.clone(), value2.clone());

    let path = create_temp_dir().unwrap().join("snapshot.bin");
    // 转储到临时文件
    // let temp_file = NamedTempFile::new().unwrap();
    // let path = temp_file.path();

    dump_cache_to_path(cache.clone(), Default::default(), path.clone())
        .await
        .expect("dump cache should succeed");

    // 创建新缓存并加载数据
    let new_cache = MyCache::new();
    load_cache_from_path(new_cache.clone(), path)
        .await
        .expect("load cache should succeed");

    // 验证数据完整性
    let loaded_value1 = new_cache.get(&key1);
    let loaded_value2 = new_cache.get(&key2);

    assert!(loaded_value1.is_some(), "key1 should exist");
    assert!(loaded_value2.is_some(), "key2 should exist");

    let v1 = loaded_value1.unwrap();
    let v2 = loaded_value2.unwrap();

    assert_eq!(v1.data.as_ref(), value1.data.as_ref());
    assert_eq!(v1.ttl_ms, value1.ttl_ms);

    assert_eq!(v2.data.as_ref(), value2.data.as_ref());
    assert_eq!(v2.ttl_ms, value2.ttl_ms);
}
