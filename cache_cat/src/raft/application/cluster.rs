use crate::error::{CacheCatError, Error, ProtocolError, StorageError};
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::{Node, NodeId, Raft, TypeConfig};
use openraft::ReadPolicy::LeaseRead;
use openraft::alias::{JoinErrorOf, VoteOf};
use openraft::async_runtime::WatchReceiver;
use openraft::base::BoxStream;
use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::raft::linearizable_read::Linearizer;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    VoteRequest, VoteResponse, WriteResult,
};
use openraft::{ChangeMembers, Instant, ReadPolicy, Snapshot, WatchChangeHandle};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU16;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeState {
    election_timeout_max: u64,
    nodes_heartbeat: HashMap<NodeId, Duration>,
}

impl NodeState {
    pub fn new(nodes_heartbeat: HashMap<NodeId, Duration>, election_timeout_max: u64) -> Self {
        Self {
            nodes_heartbeat,
            election_timeout_max,
        }
    }
    pub fn is_survive(&self, node_id: NodeId) -> bool {
        self.nodes_heartbeat
            .get(&node_id)
            .map(|d| (d.as_millis() as u64) < self.election_timeout_max)
            .unwrap_or(false)
    }
}

pub struct Cluster {
    current_endpoint: Endpoint,
    watch_change_handle: WatchChangeHandle<TypeConfig>,
    last_master: Arc<AtomicU16>,
    node_state: Arc<RwLock<NodeState>>,
    raft: Raft,
}

impl Cluster {
    pub async fn is_survive(&self, node_id: NodeId) -> bool {
        self.node_state.read().await.is_survive(node_id)
    }
    pub fn new(raft: Raft, current_endpoint: Endpoint) -> Self {
        let election_timeout_max = raft.config().election_timeout_max;

        let last_master = Arc::new(AtomicU16::new(raft.node_id().clone()));
        let last = last_master.clone();
        let watch_change_handle = raft.on_cluster_leader_change(move |_old, new| {
            let last = last.clone();
            async move {
                // 假设 LeaderId 有 node_id 字段，并且是 u16 类型
                last.store(new.0.node_id, std::sync::atomic::Ordering::Relaxed);
            }
        });
        Self {
            node_state: Arc::new(RwLock::new(NodeState::new(
                HashMap::new(),
                election_timeout_max,
            ))),
            current_endpoint,
            watch_change_handle,
            last_master,
            raft,
        }
    }

    pub async fn set_nodes_state(&self, node_state: NodeState) {
        let mut guard = self.node_state.write().await;
        *guard = node_state;
    }

    pub fn nodes_state(&self) -> Option<NodeState> {
        let heartbeat_info = self.raft.metrics().borrow_watched().heartbeat.clone()?;
        let mut node_state_map = HashMap::new();
        for (node_id, instant) in heartbeat_info {
            match instant {
                None => continue,
                Some(v) => {
                    node_state_map.insert(node_id, v.elapsed());
                }
            }
        }
        node_state_map.insert(self.node_id().clone(), Duration::from_secs(0));
        Some(NodeState::new(
            node_state_map,
            self.raft.config().election_timeout_max,
        ))
    }

    pub fn last_slave(&self) -> Vec<Node> {
        let last_master_id = self.last_master.load(std::sync::atomic::Ordering::Relaxed);
        let mut res = Vec::new();
        for (node_id, node) in self.nodes() {
            if node_id != last_master_id {
                res.push(node);
            }
        }
        res
    }

    //如果没有选出过leader就返回自己
    pub fn last_leader(&self) -> Endpoint {
        let node_id = self.last_master.load(std::sync::atomic::Ordering::Relaxed);
        let metrics_guard = self.raft.metrics();
        let metrics = metrics_guard.borrow_watched();
        metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == node_id)
            .map(|(_, node)| node.endpoint.clone())
            .unwrap_or_else(|| self.current_endpoint.clone())
    }
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    pub fn node_id(&self) -> NodeId {
        self.raft.node_id().clone()
    }

    pub async fn leader_addr(&self) -> Option<Endpoint> {
        let leader_id = self.raft.current_leader().await?;
        for (node_id, node) in self.nodes() {
            if node_id == leader_id {
                return Some(node.endpoint);
            }
        }
        None
    }

    pub fn nodes(&self) -> Vec<(NodeId, Node)> {
        let metrics_guard = self.raft.metrics();
        let metrics = metrics_guard.borrow_watched();
        let vec = metrics
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<Vec<_>>();
        vec
    }

    pub async fn initialize(&self, members: BTreeMap<NodeId, Node>) -> Result<(), CacheCatError> {
        if let Err(e) = self.raft.initialize(members).await {
            match e {
                RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(e) => {
                        info!("Already initialized: {}", e);
                    }
                    InitializeError::NotInMembers(e) => {
                        return Err(Error::config(e.to_string()));
                    }
                },
                RaftError::Fatal(e) => {
                    return Err(Error::internal(e.to_string()));
                }
            }
        }
        Ok(())
    }

    pub async fn trigger_snapshot(&self) -> Result<(), CacheCatError> {
        self.raft.trigger().snapshot().await.map_err(|e| {
            error!("snapshot error: {}", e);
            ProtocolError::Custom("snapshot error")
        })?;
        Ok(())
    }
    #[inline]
    pub async fn get_read_linearizer(
        &self,
        read_policy: ReadPolicy,
    ) -> Result<Linearizer<TypeConfig>, RaftError<TypeConfig, LinearizableReadError<TypeConfig>>>
    {
        self.raft.get_read_linearizer(read_policy).await
    }
    #[inline]
    pub async fn lease_read(&self) -> Result<(), CacheCatError> {
        let linearizer = self
            .raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&self.raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        Ok(())
    }

    #[inline]
    pub async fn install_full_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
    ) -> Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>> {
        self.raft.install_full_snapshot(vote, snapshot).await
    }

    #[inline]
    pub fn voter_ids(&self) -> impl Iterator<Item = NodeId> {
        self.raft.voter_ids()
    }

    #[inline]
    pub async fn remove_self(
        &self,
    ) -> Result<(), RaftError<TypeConfig, ClientWriteError<TypeConfig>>> {
        // 使用 AddVoters 而不是传入完整集合
        // 这会自动计算并添加到现有成员中

        let mut set: BTreeSet<NodeId> = BTreeSet::new();
        set.insert(self.node_id());
        let changes: ChangeMembers<TypeConfig> = ChangeMembers::RemoveVoters(set);
        self.raft.change_membership(changes, true).await?;

        let mut set: BTreeSet<NodeId> = BTreeSet::new();
        set.insert(self.node_id());
        let changes: ChangeMembers<TypeConfig> = ChangeMembers::RemoveNodes(set);
        self.raft.change_membership(changes, true).await?;
        Ok(())
    }
    #[inline]
    pub async fn shutdown(&self) -> Result<(), JoinErrorOf<TypeConfig>> {
        self.raft.shutdown().await
    }

    #[inline]
    pub async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<TypeConfig>>,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.change_membership(members, true).await
    }

    #[inline]
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.add_learner(node_id, node, true).await
    }

    #[inline]
    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.append_entries(rpc).await
    }

    #[inline]
    pub async fn client_write(
        &self,
        request: Request,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.client_write(request).await
    }
    #[inline]
    pub async fn client_write_many(
        &self,
        request: Vec<Request>,
    ) -> Result<
        BoxStream<'static, Result<WriteResult<TypeConfig>, Fatal<TypeConfig>>>,
        Fatal<TypeConfig>,
    > {
        self.raft.client_write_many(request).await
    }

    #[inline]
    pub async fn vote(
        &self,
        request: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.vote(request).await
    }
}
