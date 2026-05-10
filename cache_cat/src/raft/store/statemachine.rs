use crate::node::parsed_config::ParsedConfig;
use crate::protocol::NO_EXPIRATION;
use crate::protocol::key::del::DelParams;
use crate::protocol::key::rename::RenameParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::{Expiration, SetMode, SetParams};
use crate::raft::store::snapshot::snapshot_handler::{
    dump_cache_to_path, get_snapshot_file_name, load_cache_from_path,
};
use crate::raft::types::core::moka::moka::{MyCache, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, InsertReq, SetReq};
use crate::raft::types::entry::request::{AtomicRequest, RedisOperation, Request};
use crate::raft::types::file_operator::FileOperator;
use crate::raft::types::raft_types::{NodeId, TypeConfig};
use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, SnapshotMeta};
use openraft::{OptionalSend, Snapshot, StoredMembership};
use openraft::{RaftSnapshotBuilder, RaftTypeConfig};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};

//快照存在三个阶段，开始，收尾，结束
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub enum SnapshotState {
    #[default]
    End,
    Start,
    Tail,
}

#[derive(Debug, Clone, Default)]
pub struct RaftMetaData {
    //快照状态
    pub snapshot_state: SnapshotState,

    pub last_applied_log_id: Option<LogId<TypeConfig>>,

    pub last_membership: StoredMembership<TypeConfig>,
}

impl RaftMetaData {
    pub fn snapshot_state(&self) -> bool {
        self.snapshot_state != SnapshotState::End
    }
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    pub path: PathBuf,

    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    /// State built from applying the raft logs
    pub kvs: MyCache,
    //增量日志队列
    pub incremental_operation_queue: Arc<Mutex<Vec<AtomicRequest>>>,

    // 只有俩个任务会获取这个锁，快照和raft主任务。它们都是单线程的。 启动的时候也可能被获取但这不影响性能。
    pub raft_meta_data: Arc<Mutex<RaftMetaData>>,

    pub snapshot_message: broadcast::Sender<()>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    //这里是clone了一个self 然后调用build_snapshot
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, io::Error> {
        tracing::info!("Starting snapshot...");
        let mut raft_meta = self.data.raft_meta_data.lock().await;
        if raft_meta.snapshot_state == SnapshotState::Start {
            // 经过测试，openraft保证build_snapshot在每个组中最多同时存在一个，理论上这里永远不会输出
            tracing::error!("Unexpected errors, repeated snapshots!")
        }
        //开始快照
        raft_meta.snapshot_state = SnapshotState::Start;
        drop(raft_meta);
        //快照开始 此时快照线程和raft线程同时执行 快照线程只会读取数据
        let cache = self.data.kvs.clone();
        dump_cache_to_path(
            cache,
            &self.path,
            self.data.raft_meta_data.clone(),
            self.data.incremental_operation_queue.clone(),
        )
        .await?;
        //创建快照的硬链接
        //理论上这里读取的快照可能不是这里dump的快照了，因此这里返回的metadata需要重新load
        let file = FileOperator::new(&self.path).await?;
        //正常情况不该为空如果为空就抛IO异常
        let file_operator =
            file.ok_or(io::Error::new(io::ErrorKind::Other, "snapshot is empty"))?;
        let meta_data = file_operator
            .load_meta_data()
            .await?
            .ok_or(io::Error::new(io::ErrorKind::Other, "meta data is empty"))?;
        _ = self.data.snapshot_message.send(());
        Ok(Snapshot {
            meta: meta_data,
            snapshot: file_operator,
        })
    }
}

impl StateMachineStore {
    pub async fn new(
        config: ParsedConfig,
        path: PathBuf,
        node_id: NodeId,
    ) -> Result<StateMachineStore, io::Error> {
        let (tx, _) = broadcast::channel::<()>(2);
        let cache = MyCache::new(config.db_number);
        let mut sm = Self {
            data: StateMachineData {
                snapshot_message: tx,
                kvs: cache.clone(),
                incremental_operation_queue: Arc::new(Mutex::new(Vec::new())),
                raft_meta_data: Arc::new(Mutex::new(RaftMetaData {
                    snapshot_state: SnapshotState::End,
                    last_applied_log_id: None,
                    last_membership: Default::default(),
                })),
            },
            node_id,
            path: path.clone(),
        };
        let filename = get_snapshot_file_name();
        let res = load_cache_from_path(cache, path.join("snapshot").join(filename)).await?;
        match res {
            None => {}
            Some(data) => {
                //如果有值就更新元数据
                sm.update_meta_data(data.0).await;
            }
        }
        Ok(sm)
    }
    pub async fn update_meta_data(&mut self, metadata: SnapshotMeta<TypeConfig>) {
        let mut guard = self.data.raft_meta_data.lock().await;
        guard.last_membership = metadata.last_membership;
        guard.last_applied_log_id = metadata.last_log_id;
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    //让 Raft 核心在启动或恢复时，知道状态机已经应用到哪个日志位置，以及当前有效的 membership 是什么。
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), io::Error> {
        let meta_data = self.data.raft_meta_data.lock().await;
        Ok((
            meta_data.last_applied_log_id,
            meta_data.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut raft_meta = self.data.raft_meta_data.lock().await;
        let _lock = self.data.kvs.write_lock.lock().await;
        let mut guard;
        let update_type = if raft_meta.snapshot_state == SnapshotState::Start {
            guard = self.data.incremental_operation_queue.lock().await;
            &mut UpdateType::Snapshot(&mut guard, 0)
        } else {
            &mut UpdateType::None
        };
        let mut update = Update {
            db_number: 0,
            update_type,
        };
        while let Some((entry, responder)) = entries.try_next().await? {
            raft_meta.last_applied_log_id = Some(entry.log_id);
            let st = &self.data.kvs;
            let response = match entry.payload {
                EntryPayload::Blank => {
                    for cache in &st.cache {
                        cache.run_pending_tasks()
                    }
                    Value::ok()
                }
                EntryPayload::Normal(req) => {
                    let (time, db_number) = req.split_u64();
                    let write_clock = st.set_write_clock(time);
                    match update.update_type {
                        UpdateType::Snapshot(_, count) => {
                            // 直接修改 count（因为匹配到的 count 是可变的）
                            *count = write_clock;
                        }
                        _ => {}
                    }
                    update.db_number = db_number;
                    match req {
                        Request::Base(_, base) => match base {
                            BaseOperation::Empty => Value::ok(),
                            BaseOperation::Set(set) => {
                                st.set(set, &mut update);
                                Value::ok()
                            }
                            BaseOperation::Expire(expire) => st.expire(expire, &mut update),
                            BaseOperation::LPush(l_push) => st.l_push(l_push, &mut update),
                            BaseOperation::Del(del) => st.del(del, &mut update),
                            BaseOperation::Incr(incr) => st.incr(incr, &mut update),
                            BaseOperation::Append(append) => st.append(append, &mut update),
                            BaseOperation::HSet(h_set) => st.h_set(h_set, &mut update),
                            BaseOperation::HIncr(h_get) => st.h_incr(h_get, &mut update),
                            BaseOperation::ZAdd(z_add) => st.z_add(z_add, &mut update),
                            BaseOperation::SAdd(s_add) => st.s_add(s_add, &mut update),
                            BaseOperation::Persist(persist) => st.persist(persist, &mut update),
                            BaseOperation::Insert(insert) => st.insert(insert, &mut update),
                        },
                        Request::Redis(_, redis) => match redis {
                            RedisOperation::RedisDel(del) => {
                                redis_del_hand(st, del, &mut update).await
                            }
                            RedisOperation::RedisSet(set) => {
                                redis_set_hand(st, set, &mut update, write_clock).await
                            }
                            RedisOperation::RedisMset(mset) => {
                                redis_mset_hand(st, mset, &mut update).await
                            }
                            RedisOperation::RedisRename(rename) => {
                                redis_rename_hand(st, rename, &mut update).await
                            }
                        },
                    }
                }
                EntryPayload::Membership(mem) => {
                    raft_meta.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), mem.clone());
                    Value::ok()
                }
            };
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    //这个方法必须要实现，但是从来不会被调用
    async fn begin_receiving_snapshot(&mut self) -> Result<FileOperator, io::Error> {
        Ok(Default::default())
    }

    // Raft协议强制快照文件先持久化到磁盘，然后再应用到状态机。不能实现类似Redis的直接应用到状态机。
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<TypeConfig>,
        snapshot: <TypeConfig as RaftTypeConfig>::SnapshotData,
    ) -> Result<(), io::Error> {
        tracing::info!("node {} snapshot start!!!!", self.node_id);
        let path_buf = snapshot.get_local_hard_link_buf(&self.path);
        //理论上快照一定会存在
        let res = load_cache_from_path(self.data.kvs.clone(), &path_buf)
            .await?
            .ok_or(io::Error::new(io::ErrorKind::Other, "meta data is empty"))?;
        for atomic_request in res.1 {
            let update_type = &mut UpdateType::CAS(atomic_request.version);
            let mut update = Update {
                db_number: 0,
                update_type,
            };
            match atomic_request.request {
                BaseOperation::Empty => {}
                BaseOperation::Set(set) => {
                    self.data.kvs.set(set, &mut update);
                }
                BaseOperation::Expire(expire_req) => {
                    self.data.kvs.expire(expire_req, &mut update);
                }
                BaseOperation::LPush(l_push) => {
                    self.data.kvs.l_push(l_push, &mut update);
                }
                BaseOperation::Del(del) => {
                    self.data.kvs.del(del, &mut update);
                }
                BaseOperation::Incr(incr) => {
                    self.data.kvs.incr(incr, &mut update);
                }
                BaseOperation::Append(append) => {
                    self.data.kvs.append(append, &mut update);
                }
                BaseOperation::HSet(hset) => {
                    self.data.kvs.h_set(hset, &mut update);
                }
                BaseOperation::HIncr(h_incr) => {
                    self.data.kvs.h_incr(h_incr, &mut update);
                }
                BaseOperation::ZAdd(zadd) => {
                    self.data.kvs.z_add(zadd, &mut update);
                }
                BaseOperation::SAdd(sadd) => {
                    self.data.kvs.s_add(sadd, &mut update);
                }
                BaseOperation::Persist(persist) => {
                    self.data.kvs.persist(persist, &mut update);
                }
                BaseOperation::Insert(insert) => {
                    self.data.kvs.insert(insert, &mut update);
                }
            }
        }
        self.update_meta_data(res.0).await;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
        let option = FileOperator::new(&self.path).await?;
        match option {
            None => Ok(None),
            Some(res) => {
                let meta = res
                    .load_meta_data()
                    .await?
                    .ok_or(io::Error::new(io::ErrorKind::Other, "meta data is empty"))?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: res,
                }))
            }
        }
    }
}

pub async fn redis_rename_hand(
    cache: &MyCache,
    params: RenameParams,
    update: &mut Update<'_, '_>,
) -> Value {
    let _exclusive_lock = cache.read_lock.lock().await;
    let cached = match cache.get_cache(update.db_number) {
        Err(err) => return err,
        Ok(cache) => cache,
    };
    let my_value = match cached.get(&params.key) {
        None => return Value::Error("no such key".to_string()),
        Some(value) => value,
    };
    let del = DelReq {
        key: Arc::from(params.key),
    };
    cache.del(del, update);
    let new_key: Arc<Vec<u8>> = Arc::from(params.new_key);
    let insert = InsertReq {
        key: new_key.clone(),
        value: my_value.data,
        expires_at: my_value.expires_at,
    };
    cache.insert(insert, update);
    Value::ok()
}

pub async fn redis_del_hand(
    cache: &MyCache,
    params: DelParams,
    update: &mut Update<'_, '_>,
) -> Value {
    let mut count = 0;
    let _exclusive_lock = cache.read_lock.lock().await;
    for key in params.keys {
        let del = DelReq {
            key: Arc::from(key),
        };
        match cache.del(del, update) {
            Value::Error(err) => return Value::Error(err),
            Value::Integer(num) => count = count + num,
            _ => {}
        }
    }
    Value::Integer(count)
}

pub async fn redis_mset_hand(
    cache: &MyCache,
    params: MsetParams,
    update: &mut Update<'_, '_>,
) -> Value {
    let _exclusive_lock = cache.read_lock.lock().await;
    for pair in params.pairs {
        let set = SetReq {
            key: Arc::from(pair.0),
            value: Arc::from(pair.1),
            ex_time: 0,
        };
        cache.set(set, update);
    }
    Value::ok()
}

pub async fn redis_set_hand(
    cache: &MyCache,
    params: SetParams,
    update: &mut Update<'_, '_>,
    time: u64,
) -> Value {
    // 最新的写逻辑时间
    let now = time;

    enum ExistingKey {
        None,               // Key doesn't exist
        Data(Arc<Vec<u8>>), // Key exists and is a valid string
        OtherType,          // Key exists but is not a string (Hash, etc.)
    }
    let mut existing_key = ExistingKey::None;

    // Calculate expiration timestamp in milliseconds (0 means no expiration)
    let expires_at = match params.expiration {
        Some(Expiration::KeepTTL) => {
            let cache = match cache.get_cache(update.db_number) {
                Err(err) => return err,
                Ok(cache) => cache,
            };

            // Read existing value to get its expiration time
            match cache.get(&params.key) {
                None => NO_EXPIRATION,
                Some(value) => {
                    let ttl_ms = value.expires_at;
                    existing_key = match value.data {
                        ValueObject::Int(v) => {
                            ExistingKey::Data(Arc::from(v.to_string().into_bytes()))
                        }
                        ValueObject::String(v) => ExistingKey::Data(v),
                        _ => ExistingKey::OtherType,
                    };
                    ttl_ms
                }
            }
        }
        Some(exp) => match exp {
            Expiration::Ex(seconds) => now + seconds * 1000,
            Expiration::Px(millis) => now + millis,
            Expiration::ExAt(timestamp) => timestamp * 1000,
            Expiration::PxAt(timestamp) => timestamp,
            Expiration::KeepTTL => unreachable!(), // Handled above
        },
        None => NO_EXPIRATION, // No expiration
    };
    let key_exists = matches!(existing_key, ExistingKey::Data(_) | ExistingKey::OtherType);

    // Apply NX/XX mode logic
    match params.mode {
        Some(SetMode::Nx) => {
            // NX: Only set if key does not exist
            if key_exists {
                // Key exists, do not set
                return if params.get {
                    // GET with NX: return current value if it's a string, otherwise nil
                    match existing_key {
                        ExistingKey::Data(v) => Value::BulkString(Some(v.as_ref().clone())),
                        _ => Value::BulkString(None), // Other type, return nil
                    }
                } else {
                    // Just return nil (nil bulk string)
                    Value::BulkString(None)
                };
            }
        }
        Some(SetMode::Xx) => {
            // XX: Only set if key exists
            if !key_exists {
                // Key does not exist, do not set
                return if params.get {
                    // GET with XX: return nil since key doesn't exist
                    Value::BulkString(None)
                } else {
                    Value::BulkString(None)
                };
            }
        }
        None => {
            // No mode restriction, always set
        }
    }
    let set = SetReq {
        key: Arc::from(params.key),
        value: Arc::from(params.value),
        ex_time: expires_at,
    };
    cache.set(set, update);
    if params.get {
        // Store the old value for GET option before we overwrite
        match existing_key {
            ExistingKey::Data(v) => Value::BulkString(Some(v.as_ref().clone())),
            _ => Value::BulkString(None), // Other type, return nil
        }
    } else {
        Value::ok()
    }
}
