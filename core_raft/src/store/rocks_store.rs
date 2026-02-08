use std::collections::{BTreeMap, HashMap};
use std::io;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use crate::network::model::{Request, Response};
use crate::server::handler::model::SetRes;
use crate::store::rocks_log_store::RocksLogStore;
use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, SnapshotMeta};
use openraft::{OptionalSend, Snapshot, StoredMembership};
use openraft::{RaftSnapshotBuilder, RaftTypeConfig};
use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Options;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;
use crate::network::node::TypeConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: openraft::SnapshotMeta<TypeConfig>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    /// snapshot index is not persisted in this example.
    ///
    /// It is only used as a suffix of snapshot id, and should be globally unique.
    /// In practice, using a timestamp in micro-second would be good enough.
    snapshot_idx: u64,

    /// State machine stores snapshot in db.
    db: Arc<DB>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<TypeConfig>>,

    pub last_membership: openraft::StoredMembership<TypeConfig>,

    /// State built from applying the raft logs
    pub kvs: Arc<Mutex<HashMap<String, String>>>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, io::Error> {
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let kv_json = {
            let kvs = self.data.kvs.lock().await;
            bincode2::serialize(&*kvs).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!(
                "{}-{}-{}",
                last.committed_leader_id(),
                last.index(),
                self.snapshot_idx
            )
        } else {
            format!("--{}", self.snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: kv_json.clone(),
        };

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(kv_json),
        })
    }
}

impl StateMachineStore {
    async fn new(db: Arc<DB>) -> Result<StateMachineStore, io::Error> {
        let mut sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                kvs: Arc::new(Mutex::new(HashMap::new())),
            },
            snapshot_idx: 0,
            db,
        };

        let snapshot = sm.get_current_snapshot_()?;
        if let Some(snap) = snapshot {
            //当存在快照的时候才会恢复状态机
            sm.update_state_machine_(snap).await?;
        }
        Ok(sm)
    }

    //
    async fn update_state_machine_(&mut self, snapshot: StoredSnapshot) -> Result<(), io::Error> {
        let kvs: HashMap<String, String> = bincode2::deserialize(&snapshot.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();
        let mut x = self.data.kvs.lock().await;
        *x = kvs;
        Ok(())
    }

    fn get_current_snapshot_(&self) -> Result<Option<StoredSnapshot>, io::Error> {
        Ok(self
            .db
            .get_cf(self.store(), b"snapshot")
            .map_err(io::Error::other)?
            .and_then(|v| bincode2::deserialize::<StoredSnapshot>(&v).ok()))
    }

    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> Result<(), io::Error> {
        self.db
            .put_cf(
                self.store(),
                b"snapshot",
                bincode2::serialize(&snap).unwrap().as_slice(),
            )
            .map_err(io::Error::other)?;
        self.db.flush_wal(true).map_err(io::Error::other)?;
        Ok(())
    }

    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), io::Error> {
        Ok((
            self.data.last_applied_log_id,
            self.data.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        use std::time::Instant;

        let start_time = Instant::now();
        let result = async {
            while let Some((entry, responder)) = entries.try_next().await? {
                self.data.last_applied_log_id = Some(entry.log_id);

                let response = match &entry.payload {
                    EntryPayload::Blank => Response::none(),
                    EntryPayload::Normal(req) => match req {
                        Request::Set(set_req) => {
                            // 使用结构体的字段名来访问成员
                            let mut st = self.data.kvs.lock().await;
                            // println!("set {} = {}", set_req.key,String::from_utf8(set_req.value.clone()).unwrap());
                            st.insert(
                                set_req.key.clone(),
                                String::try_from(set_req.value.clone()).unwrap(),
                            );
                            // 注意：原代码返回的是 value.clone()，现在根据你的业务需求可能需要调整
                            Response::Set(SetRes {})
                        }
                    },
                    EntryPayload::Membership(mem) => {
                        self.data.last_membership =
                            StoredMembership::new(Some(entry.log_id.clone()), mem.clone());
                        Response::none()
                    }
                };

                if let Some(responder) = responder {
                    responder.send(response);
                }
            }
            Ok(())
        }
        .await;

        let elapsed = start_time.elapsed();
        tracing::info!("完成执行 apply 操作，耗时: {:?} 微秒", elapsed.as_micros());

        result
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: <TypeConfig as RaftTypeConfig>::SnapshotData,
    ) -> Result<(), io::Error> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.update_state_machine_(new_snapshot.clone()).await?;
        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        }))
    }
}

pub(crate) async fn new_storage<P: AsRef<Path>>(db_path: P) -> (RocksLogStore, StateMachineStore) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    //设置常见的优化

    db_opts
        .set_max_background_jobs((std::thread::available_parallelism().unwrap().get() / 2) as i32); //def 2
    db_opts.set_enable_pipelined_write(true); // 启用流水线写入，并发大时写入性能更高
    //l0
    db_opts.set_level_zero_file_num_compaction_trigger(8); //默认是4
    db_opts.set_level_zero_slowdown_writes_trigger(40); //默认20
    db_opts.set_level_zero_stop_writes_trigger(48); //def 24
    db_opts.set_target_file_size_base(128 * 1024 * 1024); //默认为64M

    let store = ColumnFamilyDescriptor::new("store", db_opts.clone());
    let meta = ColumnFamilyDescriptor::new("meta", db_opts.clone());
    let logs = ColumnFamilyDescriptor::new("logs", db_opts.clone());
    //打开多个数据库并创建列族
    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![store, meta, logs]).unwrap();
    let db = Arc::new(db);

    let log_store = RocksLogStore::new(db.clone());
    let sm_store = StateMachineStore::new(db).await.unwrap();

    (log_store, sm_store)
}
