use crate::network::model::{AtomicRequest, Request, Response};
use crate::network::node::{GroupId, NodeId, TypeConfig};
use crate::server::client::file_client::FileOperator;
use crate::server::core::config::get_snapshot_file_name;
use crate::server::core::moka::{MyCache, MyValue};
use crate::server::handler::model::SetRes;
use crate::store::snapshot_handler::{dump_cache_to_path, load_cache_from_path};
use futures::Stream;
use futures::TryStreamExt;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::{EntryPayload, LogId, SnapshotMeta};
use openraft::{OptionalSend, Snapshot, StoredMembership};
use openraft::{RaftSnapshotBuilder, RaftTypeConfig};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

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
#[derive(Debug, Clone, Default)]
pub struct RaftMetaData {
    //快照状态 true为开始 false为结束
    pub snapshot_state: bool,

    pub last_applied_log_id: Option<LogId<TypeConfig>>,

    pub last_membership: StoredMembership<TypeConfig>,
}

#[derive(Debug, Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    pub path: PathBuf,

    pub node_id: NodeId,
    group_id: GroupId,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    /// State built from applying the raft logs
    pub kvs: MyCache,
    //增量日志队列
    pub incremental_operation_queue: Arc<Mutex<Vec<AtomicRequest>>>,

    // 只有俩个任务会获取这个锁，快照和raft主任务。它们都是单线程的。 启动的时候也可能被获取但这不影响性能。
    raft_meta_data: Arc<Mutex<RaftMetaData>>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    //这里是clone了一个self 然后调用build_snapshot
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, io::Error> {
        let mut raft_meta = self.data.raft_meta_data.lock().await;
        if raft_meta.snapshot_state {
            // 经过测试，openraft保证build_snapshot在每个组中最多同时存在一个，理论上这里永远不会输出
            tracing::error!("Unexpected errors, repeated snapshots!")
        }
        //开始快照
        raft_meta.snapshot_state = true;
        drop(raft_meta);
        //快照开始 此时快照线程和raft线程同时执行 快照线程只会读取数据
        let cache = self.data.kvs.clone();
        dump_cache_to_path(
            cache,
            &self.path,
            self.group_id,
            self.data.raft_meta_data.clone(),
            self.data.incremental_operation_queue.clone(),
        )
        .await?;
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
    pub async fn new(
        path: PathBuf,
        group_id: GroupId,
        node_id: NodeId,
    ) -> Result<StateMachineStore, io::Error> {
        let cache = MyCache::new();
        let mut sm = Self {
            data: StateMachineData {
                kvs: cache.clone(),
                incremental_operation_queue: Arc::new(Mutex::new(Vec::new())),
                raft_meta_data: Arc::new(Mutex::new(RaftMetaData {
                    snapshot_state: false,
                    last_applied_log_id: None,
                    last_membership: Default::default(),
                })),
            },
            node_id,
            path: path.clone(),
            group_id,
        };
        let filename = get_snapshot_file_name(group_id);
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
        if raft_meta.snapshot_state {
            let mut operation_queue = self.data.incremental_operation_queue.lock().await;
            while let Some((entry, responder)) = entries.try_next().await? {
                raft_meta.last_applied_log_id = Some(entry.log_id);
                let response = match entry.payload {
                    EntryPayload::Blank => Response::none(),
                    EntryPayload::Normal(req) => match req {
                        Request::Set(set_req) => {
                            // 使用结构体的字段名来访问成员
                            let st = &self.data.kvs;
                            st.snapshot_set(set_req, &mut operation_queue).await;
                            Response::Set(SetRes {})
                        }
                    },
                    EntryPayload::Membership(mem) => {
                        raft_meta.last_membership =
                            StoredMembership::new(Some(entry.log_id.clone()), mem.clone());
                        Response::none()
                    }
                };
                if let Some(responder) = responder {
                    responder.send(response);
                }
            }
        } else {
            while let Some((entry, responder)) = entries.try_next().await? {
                raft_meta.last_applied_log_id = Some(entry.log_id);
                let response = match entry.payload {
                    EntryPayload::Blank => Response::none(),
                    EntryPayload::Normal(req) => match req {
                        Request::Set(set_req) => {
                            // 使用结构体的字段名来访问成员
                            let st = &self.data.kvs;
                            st.set(set_req).await;
                            Response::Set(SetRes {})
                        }
                    },
                    EntryPayload::Membership(mem) => {
                        raft_meta.last_membership =
                            StoredMembership::new(Some(entry.log_id.clone()), mem.clone());
                        Response::none()
                    }
                };
                if let Some(responder) = responder {
                    responder.send(response);
                }
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
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: <TypeConfig as RaftTypeConfig>::SnapshotData,
    ) -> Result<(), io::Error> {
        tracing::warn!("node {} snapshot start!!!!", self.node_id);
        let path_buf = snapshot.get_local_hard_link_buf(&self.path);
        //理论上快照一定会存在
        let res = load_cache_from_path(self.data.kvs.clone(), &path_buf)
            .await?
            .ok_or(io::Error::new(io::ErrorKind::Other, "meta data is empty"))?;
        for atomic_request in res.1 {
            match atomic_request.request {
                Request::Set(set_req) => {
                    self.data
                        .kvs
                        .cas_set(set_req, atomic_request.version)
                        .await;
                }
            }
        }
        self.update_meta_data(res.0).await;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, io::Error> {
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
