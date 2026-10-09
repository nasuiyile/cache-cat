use crate::raft::store::statemachine::RaftMetaData;
use crate::raft::store::statemachine::SnapshotState::{End, Tail};
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::entry::request::AtomicRequest;
use crate::raft::types::raft_types::SnapshotMeta;
use serde::{Deserialize, Serialize};
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

pub(crate) const VERSION: u8 = 1;

// 预填充占位符
const PLACEHOLDER_LENGTH: usize = 300;

pub const SNAPSHOT_FILE_NAME: &str = "snapshot";

pub fn get_snapshot_file_name() -> String {
    format!("{}.bin", SNAPSHOT_FILE_NAME)
}

#[derive(Serialize, Deserialize)]
pub(crate) struct CacheCatSnapshotMeta {
    pub meta: SnapshotMeta,
    pub write_clock: u64,
    pub snapshot_revision: u64,
}

pub async fn dump_cache_to_path<P>(
    cache: Arc<MyCache>,
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
    writer.write_all(&[0u8; PLACEHOLDER_LENGTH]).await?;

    cache.dump_cache_to_writer(&mut writer).await?;

    // Enter the post-snapshot tail. Operations after this point are normal
    // Raft log applications and are recovered from logs after last_log_id;
    // only operations observed while in Start belong to this queue.
    let mut raft_meta_data = raft_meta.lock().await;
    raft_meta_data.snapshot_state = Tail;
    let pending = {
        let mut guard = queue.lock().await;
        std::mem::take(&mut *guard)
    };
    let snapshot_meta = SnapshotMeta {
        last_log_id: raft_meta_data.last_applied_log_id,
        last_membership: raft_meta_data.last_membership.clone(),
        // snapshot_id: "".into(),
    };
    let cache_cat_snapshot_meta = CacheCatSnapshotMeta {
        meta: snapshot_meta,
        write_clock: cache.get_write_clock(),
        snapshot_revision: raft_meta_data.snapshot_revision,
    };
    let result = bincode2::serialize(&cache_cat_snapshot_meta)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if result.len() + 4 > PLACEHOLDER_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot metadata too large",
        ));
    }
    drop(raft_meta_data);
    //回填数据
    writer.seek(SeekFrom::Start(5)).await?;
    writer.write_u32(result.len() as u32).await?;
    writer.write_all(&result).await?;
    writer.seek(SeekFrom::End(0)).await?;
    write_operation_queue_to_writer(&mut writer, &pending).await?;
    writer.flush().await?;
    writer.get_ref().sync_all().await?;
    // 在现在版本的rust中，windows下rename最终会替换现有文件，因此没必要先remove
    fs::rename(&temp_path, &final_path).await?;
    raft_meta.lock().await.snapshot_state = End;
    Ok(())
}

pub async fn load_cache_from_path<P>(
    cache: Arc<MyCache>,
    path: P,
) -> Result<Option<(SnapshotMeta, Vec<AtomicRequest>, u64, u64)>, io::Error>
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
        return Err(io::Error::other("invalid file magic"));
    }

    let version = reader.read_u8().await?;
    if version != VERSION {
        return Err(io::Error::other("unsupported version"));
    }

    let meta_len = reader.read_u32().await? as usize;
    if meta_len > PLACEHOLDER_LENGTH - 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot metadata length exceeds placeholder",
        ));
    }
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: CacheCatSnapshotMeta = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if meta.snapshot_revision == u64::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid snapshot revision",
        ));
    }
    reader
        .seek(SeekFrom::Current(
            (PLACEHOLDER_LENGTH - (4 + meta_len)) as i64,
        ))
        .await?;
    //解压快照数据
    cache.load_cache_from_reader(&mut reader).await?;
    //加载缓存下来的队列操作，但是不立即执行
    let queue = load_operation_queue_from_reader(&mut reader).await?;

    // Deletions and FLUSH operations also consume revisions, so the surviving
    // values alone cannot reconstruct this watermark. Both startup and snapshot
    // installation must restore it before another snapshot can allocate IDs.
    // Publish the final clock only after the incremental queue has replayed.
    Ok(Some((
        meta.meta,
        queue,
        meta.write_clock,
        meta.snapshot_revision,
    )))
}
pub async fn dump_operation_queue_to_writer<W>(
    writer: &mut W,
    queue: Arc<Mutex<Vec<AtomicRequest>>>,
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin + Send,
{
    let queue = {
        let mut guard = queue.lock().await;
        std::mem::take(&mut *guard)
    };
    for request in queue.iter() {
        let request_bytes = bincode2::serialize(&request)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer.write_u64(request_bytes.len() as u64).await?;
        writer.write_all(&request_bytes).await?;
    }
    writer.write_u64(0).await?;
    Ok(())
}

async fn write_operation_queue_to_writer<W>(
    writer: &mut W,
    queue: &[AtomicRequest],
) -> Result<(), io::Error>
where
    W: AsyncWrite + Unpin + Send,
{
    for request in queue {
        let request_bytes = bincode2::serialize(request)
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
        let opt_len = reader.read_u64().await? as usize;
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

#[tokio::test]
async fn test_dump_and_load_with_data() {
    use bytes::Bytes;

    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::ValueObject;
    let cache = Arc::new(MyCache::new(1).unwrap());

    // 插入测试数据
    let key1 = Bytes::from_static(b"key1");
    let value1 = MyValue {
        version: 0,
        data: ValueObject::String(Bytes::from_static(b"value1")),
    };

    let key2 = Bytes::from_static(b"key2");
    let value2 = MyValue {
        version: 0,
        data: ValueObject::String(Bytes::from_static(b"value2")),
    };

    cache.databases[0]
        .mocha
        .insert_persistent(key1.clone(), value1.clone());
    cache.databases[0]
        .mocha
        .insert_persistent(key2.clone(), value2.clone());
    // let req = SetReq {
    //     key: Vec::from("xxx").into(),
    //     value: Vec::from("xxx").into(),
    //     ex_time: 0,
    // };
    // let mut opt_queue = Vec::new();
    // cache.snapshot_insert(req, &mut opt_queue).await;
    // Use the platform's temporary directory so this test is portable.
    let path = tempfile::Builder::new()
        .suffix("_1")
        .tempdir()
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
    let new_cache = Arc::new(MyCache::new(1).unwrap());
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
    let loaded_value1 = new_cache.databases[0].mocha.get(&key1);
    let loaded_value2 = new_cache.databases[0].mocha.get(&key2);

    assert!(loaded_value1.is_some(), "key1 should exist");
    assert!(loaded_value2.is_some(), "key2 should exist");

    let v1 = loaded_value1.unwrap();
    let v2 = loaded_value2.unwrap();

    match (&v1.data, &value1.data) {
        (ValueObject::String(a), ValueObject::String(b)) => {
            assert_eq!(a.as_ref(), b.as_ref(), "key1 value mismatch");
        }
        _ => panic!("key1 type mismatch"),
    }

    match (&v2.data, &value2.data) {
        (ValueObject::String(a), ValueObject::String(b)) => {
            assert_eq!(a.as_ref(), b.as_ref(), "key2 value mismatch");
        }
        _ => panic!("key2 type mismatch"),
    }
}

#[tokio::test]
async fn empty_snapshots_preserve_revision_watermark_across_restarts() {
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::protocol::key::del::DelReq;
    use crate::protocol::key::flushall::FlushAllReq;
    use crate::protocol::key::flushdb::FlushDBReq;
    use crate::raft::store::statemachine::{SnapshotState, StateMachineStore};
    use crate::raft::types::core::mocha::core::{
        MyValue, Update, UpdateType, next_snapshot_revision,
    };
    use crate::raft::types::core::mocha::request_handler::base_request;
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::base_operation::BaseOperation;
    use crate::raft::types::file_operator::FileOperator;
    use bytes::Bytes;

    let key = Bytes::from_static(b"removed");
    let removals = [
        BaseOperation::Del(DelReq { key: key.clone() }),
        BaseOperation::FlushDB(FlushDBReq { async_mode: false }),
        BaseOperation::FlushAll(FlushAllReq { async_mode: false }),
    ];
    for removal in removals {
        let path = tempfile::tempdir().unwrap();
        let mut cache = Arc::new(MyCache::new(1).unwrap());
        let mut revision = 40;
        let mut config = Config::default();
        config.redis.databases = 1;
        let config = ParsedConfig::from(&config).unwrap();

        // Run two successive snapshots separated by a simulated process restart.
        // Each snapshot ends with deletion, so no surviving value can provide
        // the counter's high watermark to the loader.
        for expected_revision in [41, 42] {
            cache.databases[0].mocha.insert_persistent(
                key.clone(),
                MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            );
            let mut pending = Vec::new();
            let mut update_type = UpdateType::Snapshot {
                queue: &mut pending,
                revision: &mut revision,
            };
            let mut update = Update {
                db_number: 0,
                write_clock: cache.set_write_clock(100),
                update_type: &mut update_type,
            };
            base_request(&cache, removal.clone(), &mut update);
            assert_eq!(cache.databases[0].mocha.len(), 0);
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].version, expected_revision);

            let raft_meta = Arc::new(Mutex::new(RaftMetaData {
                snapshot_state: SnapshotState::Start,
                snapshot_revision: revision,
                ..Default::default()
            }));
            dump_cache_to_path(
                cache.clone(),
                path.path(),
                raft_meta,
                Arc::new(Mutex::new(pending)),
            )
            .await
            .unwrap();

            let file = FileOperator::new(path.path()).await.unwrap().unwrap();
            assert!(file.load_meta_data().await.unwrap().is_some());
            cache = Arc::new(MyCache::new(1).unwrap());
            let (_, pending, final_clock, restored_revision) =
                load_cache_from_path(cache.clone(), file.get_hard_link_buf())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].version, expected_revision);
            assert_eq!(final_clock, 100);
            assert_eq!(cache.databases[0].mocha.len(), 0);
            assert_eq!(restored_revision, expected_revision);

            // Exercise the real startup path as well as the snapshot reader.
            let store = StateMachineStore::new(config.clone(), path.path().to_path_buf(), 1)
                .await
                .unwrap();
            cache = store.data.kvs.clone();
            revision = store.data.raft_meta_data.lock().await.snapshot_revision;
            assert_eq!(revision, expected_revision);
            assert_eq!(cache.databases[0].mocha.len(), 0);
        }
        assert_eq!(next_snapshot_revision(&mut revision), 43);
    }
}

#[tokio::test]
async fn install_snapshot_replays_persist_before_resuming_expiration() {
    use crate::protocol::key::persist::PersistReq;
    use crate::raft::store::statemachine::{StateMachineData, StateMachineStore};
    use crate::raft::types::core::mocha::core::{MyValue, next_snapshot_revision};
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::base_operation::BaseOperation;
    use crate::raft::types::file_operator::FileOperator;
    use crate::utils::OptionalU64;
    use bytes::Bytes;
    use openraft::storage::RaftStateMachine;
    use tokio::sync::broadcast;

    let path = tempfile::tempdir().unwrap();
    let snapshot_dir = path.path().join("snapshot");
    fs::create_dir_all(&snapshot_dir).await.unwrap();
    let full = MyCache::new(1).unwrap();
    let persist_key = Bytes::from_static(b"persist");
    let future_key = Bytes::from_static(b"future");
    for (key, expire_at) in [
        (persist_key.clone(), 500),
        (Bytes::from_static(b"due"), 500),
        (future_key.clone(), 1_500),
    ] {
        full.databases[0].mocha.insert_absolute(
            key,
            MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at,
        );
    }

    // The full pass saw the old TTL, then PERSIST ran at 400 before the
    // snapshot's final clock reached 1000.
    let meta = CacheCatSnapshotMeta {
        meta: SnapshotMeta {
            last_log_id: None,
            last_membership: Default::default(),
        },
        write_clock: 1_000,
        snapshot_revision: 2,
    };
    let meta_bytes = bincode2::serialize(&meta).unwrap();
    let mut writer = BufWriter::new(
        File::create(snapshot_dir.join(get_snapshot_file_name()))
            .await
            .unwrap(),
    );
    writer.write_all(CACHE_MAGIC_NUM).await.unwrap();
    writer.write_u8(VERSION).await.unwrap();
    writer.write_u32(meta_bytes.len() as u32).await.unwrap();
    writer.write_all(&meta_bytes).await.unwrap();
    writer
        .write_all(&vec![0; PLACEHOLDER_LENGTH - 4 - meta_bytes.len()])
        .await
        .unwrap();
    full.dump_cache_to_writer(&mut writer).await.unwrap();
    write_operation_queue_to_writer(
        &mut writer,
        &[AtomicRequest {
            request: BaseOperation::Persist(PersistReq {
                key: persist_key.clone(),
            }),
            version: 2,
            expected_revision: OptionalU64::some(0),
            write_clock: 400,
            db_number: 0,
        }],
    )
    .await
    .unwrap();
    writer.flush().await.unwrap();
    writer.get_ref().sync_all().await.unwrap();
    drop(writer);

    let snapshot = FileOperator::new(path.path()).await.unwrap().unwrap();
    fs::copy(
        snapshot.get_hard_link_buf(),
        snapshot.get_local_hard_link_buf(path.path()),
    )
    .await
    .unwrap();
    let cache = Arc::new(MyCache::new(1).unwrap());
    cache.set_write_clock(2_000);
    cache.databases[0].mocha.active_expire_cycle_blocking();
    let mut store = StateMachineStore {
        data: StateMachineData {
            kvs: cache.clone(),
            incremental_operation_queue: Default::default(),
            raft_meta_data: Default::default(),
            snapshot_message: broadcast::channel(2).0,
        },
        path: path.path().to_path_buf(),
        node_id: 1,
    };
    store.install_snapshot(&meta.meta, snapshot).await.unwrap();

    assert_eq!(cache.get_write_clock(), 1_000);
    {
        let mut metadata = store.data.raft_meta_data.lock().await;
        assert_eq!(metadata.snapshot_revision, 2);
        assert_eq!(next_snapshot_revision(&mut metadata.snapshot_revision), 3);
    }
    let mocha = &cache.databases[0].mocha;
    // Check physical deletion before any read can perform lazy expiration.
    assert_eq!(mocha.len(), 2);
    let persisted = mocha.get_entry(&persist_key).unwrap();
    assert_eq!(persisted.expire_at, None);
    assert_eq!(persisted.value.version, 2);
    assert_eq!(mocha.get_entry(&future_key).unwrap().expire_at, Some(1_500));

    cache.set_write_clock(1_500);
    mocha.active_expire_cycle_blocking();
    assert_eq!(mocha.len(), 1);
    assert!(mocha.get_entry(&persist_key).is_some());
}
