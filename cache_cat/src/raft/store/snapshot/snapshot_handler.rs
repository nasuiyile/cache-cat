use crate::raft::store::statemachine::RaftMetaData;
use crate::raft::types::core::cache::moka::{MyCache, MyValue};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::request::AtomicRequest;
use crate::raft::types::raft_types::TypeConfig;
use openraft::SnapshotMeta;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::sync::Mutex;
use tokio::{fs, io};
use uuid::Uuid;

const CACHE_MAGIC_NUM: &[u8; 4] = b"MCDC";

const VERSION: u8 = 1;

pub const TEMP_PATH: &str = r"E:\tmp\raft\raft-engine";
pub const SNAPSHOT_FILE_NAME: &str = "snapshot";
pub fn get_snapshot_file_name() -> String {
    format!("{}.bin", SNAPSHOT_FILE_NAME)
}
pub async fn dump_cache_to_path<P>(
    cache: MyCache,
    path: P,
    raft_meta: Arc<Mutex<RaftMetaData>>,
    queue: Arc<Mutex<Vec<AtomicRequest>>>,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let snapshot_dir = path.join("snapshot");
    // 确保 snapshot 文件夹存在
    fs::create_dir_all(&snapshot_dir).await?;

    // 创建临时文件名
    let temp_filename = format!("snapshot_from_mem_{}.tmp", Uuid::new_v4());
    let final_filename = get_snapshot_file_name();

    let temp_path = snapshot_dir.join(&temp_filename);
    let final_path = snapshot_dir.join(&final_filename);
    tracing::info!("dump cache to {}", final_path.display());
    // 写入临时文件
    let f = File::create(&temp_path).await?;
    // 通过 with_capacity 指定缓冲区大小 如果缓冲区满了则会自动 flush，让操作系统决定刷盘时间（flush不是真正刷盘，sync才是真正刷盘）
    let mut writer = BufWriter::new(f);

    writer.write_all(CACHE_MAGIC_NUM).await?;
    writer.write_u8(VERSION).await?;
    //给meta预留300byte空间方便回填
    writer.write_all(&[0u8; 300]).await?;

    cache.dump_cache_to_writer(&mut writer).await?;
    //将所有期间的操作写入
    dump_operation_queue_to_writer(&mut writer, queue).await?;

    //在最耗时的刷盘工作开始前将快照标记为已经结束
    let mut raft_meta_data = raft_meta.lock().await;
    raft_meta_data.snapshot_state = false;
    let snapshot_meta = SnapshotMeta {
        last_log_id: raft_meta_data.last_applied_log_id.clone(),
        last_membership: raft_meta_data.last_membership.clone(),
        snapshot_id: "".into(),
    };
    let result = bincode2::serialize(&snapshot_meta)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    drop(raft_meta_data);
    //写入增量操作队列。

    writer.seek(SeekFrom::Start(5)).await?;
    writer.write_u32(result.len() as u32).await?;
    writer.write_all(&result).await?;

    writer.flush().await?;
    writer.get_ref().sync_all().await?;

    // 通过 rename 原子替换目标文件
    fs::rename(&temp_path, &final_path).await?;

    Ok(())
}

pub async fn load_cache_from_path<P>(
    cache: MyCache,
    path: P,
) -> Result<Option<(SnapshotMeta<TypeConfig>, Vec<AtomicRequest>)>, io::Error>
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
    let meta_len = reader.read_u32().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    reader
        .seek(SeekFrom::Current((300 - (4 + meta_len)) as i64))
        .await?;
    //解压快照数据
    cache.load_cache_from_reader(&mut reader).await?;
    let queue = load_operation_queue_from_reader(&mut reader).await?;

    let meta: SnapshotMeta<TypeConfig> = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some((meta, queue)))
}
pub async fn dump_operation_queue_to_writer<W>(
    writer: &mut W,
    queue: Arc<Mutex<Vec<AtomicRequest>>>,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin + Send,
{
    let queue = queue.lock().await;
    for request in queue.iter() {
        let request_bytes = bincode2::serialize(&request)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer.write_u64(request_bytes.len() as u64).await?;
        writer.write_all(&request_bytes).await?;
    }
    writer.write_u64(0).await?;
    Ok(())
}
pub async fn load_operation_queue_from_reader<R>(
    reader: &mut R,
) -> Result<Vec<AtomicRequest>, io::Error>
where
    R: AsyncRead + Unpin,
{
    let mut list = Vec::new();
    loop {
        let opt_len = match reader.read_u64().await {
            Ok(v) => v as usize,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e);
                }
            }
        };
        if opt_len == 0 {
            break;
        }
        let mut opt_buf = vec![0u8; opt_len];
        reader.read_exact(&mut opt_buf).await?;
        let request: AtomicRequest = bincode2::deserialize(&opt_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        list.push(request);
    }
    Ok(list)
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

    let meta_len = reader.read_u32().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: SnapshotMeta<TypeConfig> = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(meta))
}

#[tokio::test]
async fn test_dump_and_load_with_data() {
    let cache = MyCache::new();

    // 插入测试数据
    let key1 = Arc::new(b"key1".to_vec());
    let value1 = MyValue {
        version: 1,
        data: ValueObject::String(Arc::new(b"value1".to_vec())),
        expires_at: 1000,
    };

    let key2 = Arc::new(b"key2".to_vec());
    let value2 = MyValue {
        version: 1,
        data: ValueObject::String(Arc::new(b"value1".to_vec())),
        expires_at: 0, // 永不过期
    };

    cache.cache.insert(key1.clone(), value1.clone()).await;
    cache.cache.insert(key2.clone(), value2.clone()).await;
    // let req = SetReq {
    //     key: Vec::from("xxx").into(),
    //     value: Vec::from("xxx").into(),
    //     ex_time: 0,
    // };
    // let mut opt_queue = Vec::new();
    // cache.snapshot_insert(req, &mut opt_queue).await;
    let path = tempfile::Builder::new()
        .suffix("_1")
        .tempdir_in(TEMP_PATH)
        .unwrap()
        .keep()
        .join("");

    dump_cache_to_path(
        cache.clone(),
        path.clone(),
        Default::default(),
        Default::default(),
    )
    .await
    .expect("dump cache should succeed");

    // 创建新缓存并加载数据
    let new_cache = MyCache::new();
    match load_cache_from_path(
        new_cache.clone(),
        path.join("snapshot").join(get_snapshot_file_name()),
    )
    .await
    {
        Ok(v) => println!("load ok: {:?}", v.unwrap().1),
        Err(e) => {
            println!("load error: {:?}", e);
            return;
        }
    }

    // 验证数据完整性
    let loaded_value1 = new_cache.cache.get(&key1).await;
    let loaded_value2 = new_cache.cache.get(&key2).await;

    assert!(loaded_value1.is_some(), "key1 should exist");
    assert!(loaded_value2.is_some(), "key2 should exist");

    let v1 = loaded_value1.unwrap();
    let v2 = loaded_value2.unwrap();

    match (&v1.data, &value1.data) {
        (ValueObject::String(a), ValueObject::String(b)) => {
            assert_eq!(a.as_slice(), b.as_slice(), "key1 value mismatch");
        }
        _ => panic!("key1 type mismatch"),
    }

    assert_eq!(v1.expires_at, value1.expires_at);

    match (&v2.data, &value2.data) {
        (ValueObject::String(a), ValueObject::String(b)) => {
            assert_eq!(a.as_slice(), b.as_slice(), "key2 value mismatch");
        }
        _ => panic!("key2 type mismatch"),
    }

    assert_eq!(v2.expires_at, value2.expires_at);
}
