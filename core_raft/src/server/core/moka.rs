use byteorder::LittleEndian;
use moka::Expiry;
use moka::sync::Cache;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::error::Error;
use std::io::Write;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::{fs, io};

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
    //流式序列化和反序列化

    /// 将 cache 的条目以流式方式写入到指定文件（原子替换）
    /// 文件格式（简述）:
    /// [4 bytes magic ][u8 version=1]
    /// 然后多条记录：
    /// [u64 key_len][key_bytes][u64 val_len][val_bytes]
    /// key_bytes 与 val_bytes 都是由 bincode 序列化得到的字节
    pub async fn dump_to_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = path.as_ref();
        let tmp = path.with_extension("tmp");

        let f = File::create(&tmp).await?;
        let mut writer = BufWriter::new(f);

        // header
        writer.write_all(CACHE_MAGIC_NUM).await?;
        writer.write_all(&[1u8]).await?; // version

        // 遍历 cache 条目并逐条写入
        // 假设 moka::sync::Cache 提供 iter()，返回 (K, V) 的克隆对。
        // 若你的 moka 版本 API 不同，可根据实际改为合适的遍历方式。
        for entry in self.cache.iter() {
            let (k_arc, v) = entry; // k_arc: Arc<Vec<u8>>, v: MyValue (克隆拷贝)
            // 将 key 与 value 分别序列化
            let key_bytes = bincode2::serialize(&*k_arc)?; // *k_arc -> Vec<u8>
            let val_bytes = bincode2::serialize(&v)?;

            // 写入 key 长度与 key
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;

            // 写入 value 长度与 value
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
        }

        // 确保全部 flush，再重命名到目标路径（原子替换）
        writer.flush().await?;
        fs::rename(&tmp, path).await?;

        Ok(())
    }
    pub async fn load_from_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path = path.as_ref();
        let f = File::open(path).await?;
        let mut reader = BufReader::new(f);

        // 读取 header
        let mut magic = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut magic).await {
            return Err(Box::new(e));
        }
        if &magic != CACHE_MAGIC_NUM {
            return Err(Box::from("invalid file magic"));
        }
        // 读取版本号
        let mut version = [0u8; 1];
        reader.read_exact(&mut version).await?;
        if version[0] != 1 {
            return Err(Box::from("unsupported version"));
        }

        loop {
            // 读取 key_len（u64），若遇到 EOF 则结束循环
            let key_len = match reader.read_u64().await {
                Ok(v) => v as usize,
                Err(e) => {
                    // 如果是文件结尾，正常退出
                    if let io::ErrorKind::UnexpectedEof = e.kind() {
                        break;
                    } else {
                        return Err(Box::new(e));
                    }
                }
            };

            // 读取 key bytes
            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf).await?;

            // 读取 val_len
            let val_len = reader.read_u64().await? as usize;
            // 读取 val bytes
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf).await?;

            // 反序列化
            // key 在文件里是 Vec<u8>，我们反序列化为 Vec<u8> 并包成 Arc
            let key_vec: Vec<u8> = bincode2::deserialize(&key_buf)?;
            let value: MyValue = bincode2::deserialize(&val_buf)?;

            let key_arc = Arc::new(key_vec);

            // 插入 cache（会触发 expire_after_create）
            self.cache.insert(key_arc, value);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::core::config::create_temp_dir;
    use std::time::Duration;
    use tokio::time::sleep;

    fn make_key(data: &[u8]) -> Arc<Vec<u8>> {
        Arc::new(data.to_vec())
    }

    fn make_value(data: &[u8], ttl_ms: u64) -> MyValue {
        MyValue {
            data: Arc::new(data.to_vec()),
            ttl_ms,
        }
    }

    #[tokio::test]
    async fn test_dump_and_load_basic() {
        let cache = MyCache::new();

        let k1 = make_key(b"key1");
        let v1 = make_value(b"value1", 0);

        let k2 = make_key(b"key2");
        let v2 = make_value(b"value2", 0);

        cache.insert(k1.clone(), v1.clone());
        cache.insert(k2.clone(), v2.clone());

        let path = create_temp_dir().unwrap().join("mycache_test_dump.dat");
        // dump
        cache.dump_to_path(&path).await.unwrap();

        // 新建 cache 并 load
        let new_cache = MyCache::new();
        new_cache.load_from_path(&path).await.unwrap();

        // 验证数据一致
        let loaded_v1 = new_cache.get(&k1).unwrap();
        let loaded_v2 = new_cache.get(&k2).unwrap();

        assert_eq!(&*loaded_v1.data, b"value1");
        assert_eq!(&*loaded_v2.data, b"value2");

        // 清理文件
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_dump_and_load_with_ttl() {
        let cache = MyCache::new();

        let k = make_key(b"ttl_key");
        let v = make_value(b"ttl_value", 100); // 100ms TTL

        cache.insert(k.clone(), v.clone());

        let path = create_temp_dir().unwrap().join("mycache_test_ttl.dat");

        cache.dump_to_path(&path).await.unwrap();

        let new_cache = MyCache::new();
        new_cache.load_from_path(&path).await.unwrap();

        // 立即获取应存在
        let loaded = new_cache.get(&k);
        assert!(loaded.is_some());
        assert_eq!(&*loaded.unwrap().data, b"ttl_value");

        // 等待超过 TTL
        sleep(Duration::from_millis(150)).await;

        // 触发访问后应过期
        let expired = new_cache.get(&k);
        assert!(expired.is_none());

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_empty_cache_dump_load() {
        let cache = MyCache::new();
        let path = create_temp_dir().unwrap().join("mycache_test_empty.dat");

        cache.dump_to_path(&path).await.unwrap();

        let new_cache = MyCache::new();
        new_cache.load_from_path(&path).await.unwrap();

        // 空 cache 不应 panic
        assert!(new_cache.cache.iter().next().is_none());

        let _ = tokio::fs::remove_file(&path).await;
    }
}
