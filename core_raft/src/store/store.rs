use crate::network::model::{Request, Response};
use crate::network::node::{GroupId, TypeConfig};
use crate::server::core::moka::{MyCache, MyValue, dump_cache_to_path, load_cache_from_path};
use crate::server::handler::model::SetRes;
use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, SnapshotMeta};
use openraft::{OptionalSend, Snapshot, StoredMembership};
use openraft::{RaftSnapshotBuilder, RaftTypeConfig};

use crate::server::client::file_client::FileOperator;
use crate::server::core::config::get_snapshot_file_name;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

pub struct FileStore {
    pub path: String,
}
impl Drop for FileStore {
    fn drop(&mut self) {
        //销毁的时候如果文件存在，则删除文件
        if Path::new(&self.path).exists() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<TypeConfig>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    pub path: PathBuf,

    group_id: GroupId,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<TypeConfig>>,

    pub last_membership: StoredMembership<TypeConfig>,

    /// State built from applying the raft logs
    pub kvs: MyCache,

    pub diff_map: Arc<HashMap<Arc<Vec<u8>>, MyValue>>,
    pub snapshot_state: Arc<AtomicU8>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    //这里是clone了一个self 然后调用build_snapshot
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, io::Error> {
        //将快照标记为开始
        self.data.snapshot_state.store(1, Ordering::SeqCst);
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}", last.committed_leader_id(), last.index(),)
        } else {
            String::from("--")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let cache = self.data.kvs.clone();
        dump_cache_to_path(cache, meta.clone(), &self.path, self.group_id).await?;
        //创建快照的硬链接
        //理论上这里读取的快照可能不是这里dump的快照了，因此这里返回的metadata需要重新load
        let file = FileOperator::new(self.group_id, &self.path).await?;
        //正常情况不该为空如果为空就抛IO异常
        let file_operator =
            file.ok_or(io::Error::new(io::ErrorKind::Other, "snapshot is empty"))?;
        let meta_data = file_operator
            .load_meta_data()
            .await?
            .ok_or(io::Error::new(io::ErrorKind::Other, "meta data is empty"))?;
        Ok(Snapshot {
            meta: meta_data,
            snapshot: file_operator,
        })
    }
}

impl StateMachineStore {
    pub async fn new(path: PathBuf, group_id: GroupId) -> Result<StateMachineStore, io::Error> {
        let cache = MyCache::new();
        let sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                kvs: cache.clone(),
                diff_map: Arc::new(HashMap::new()),
                snapshot_state: Arc::new(AtomicU8::new(0)),
            },
            path: path.clone(),
            group_id,
        };
        let filename = get_snapshot_file_name(group_id);
        load_cache_from_path(cache, path.join("snapshot").join(filename)).await?;

        Ok(sm)
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    //让 Raft 核心在启动或恢复时，知道状态机已经应用到哪个日志位置，以及当前有效的 membership 是什么。
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

                let response = match entry.payload {
                    EntryPayload::Blank => Response::none(),
                    EntryPayload::Normal(req) => match req {
                        Request::Set(set_req) => {
                            // 使用结构体的字段名来访问成员
                            let st = &self.data.kvs;
                            let value = MyValue {
                                data: Arc::new(set_req.value),
                                ttl_ms: 0,
                            };
                            st.insert(Arc::new(set_req.key), value);
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
        self.clone()
    }

    //这个方法必须要实现，但是从来不会被调用
    async fn begin_receiving_snapshot(&mut self) -> Result<FileOperator, io::Error> {
        Ok(Default::default())
    }

    // Raft协议强制快照文件先持久化到磁盘，然后再应用到状态机。不能实现类似Redis的直接应用到状态机。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: <TypeConfig as RaftTypeConfig>::SnapshotData,
    ) -> Result<(), io::Error> {
        let path_buf = snapshot.get_local_hard_link_buf(&self.path);
        load_cache_from_path(self.data.kvs.clone(), &path_buf).await?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
        // println!("当前快照路径{:?}", self.path);
        let option = FileOperator::new(self.group_id, &self.path).await?;
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
