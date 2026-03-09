use crate::network::node::{GroupId, NodeId, TypeConfig};
use crate::server::client::client::{RpcClient, RpcMultiClient};
use crate::server::handler::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};

use dashmap::DashMap;
use openraft::RPCTypes::InstallSnapshot;
use openraft::alias::VoteOf;
use openraft::error::{
    RPCError, RemoteError, ReplicationClosed, StreamingError, Timeout, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkFactory, Snapshot};
use openraft_multi::{GroupNetworkAdapter, GroupNetworkFactory, GroupRouter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

pub type MultiNetworkFactory = GroupNetworkFactory<Router, GroupId>;
impl RaftNetworkFactory<TypeConfig> for MultiNetworkFactory {
    type Network = GroupNetworkAdapter<TypeConfig, GroupId, Router>;

    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        let router = self.factory.clone();
        let addr = node.addr.clone();
        let nodes = router.nodes.clone();
        match RpcMultiClient::connect(&addr, target).await {
            Ok(client) => {
                nodes.insert(target, client);
            }
            Err(_) => {
                tracing::info!("connect to node {} failed, start retrying", addr);
                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(1)).await;
                        match RpcMultiClient::connect(&addr, target).await {
                            Ok(client) => {
                                tracing::info!("reconnect to {} success", addr);
                                nodes.insert(target, client);
                                break; // 成功后退出循环
                            }
                            Err(_) => {
                                tracing::debug!("retry connect to {} failed", addr);
                            }
                        }
                    }
                });
            }
        }
        GroupNetworkAdapter::new(router, target, self.group_id.clone())
    }
}

/// Multi-Raft Router with per-node connection sharing.
#[derive(Clone, Default)]
pub struct Router {
    /// Map from node_id to node connection.
    /// 所有节点都有这个nodes副本
    pub nodes: Arc<DashMap<NodeId, RpcMultiClient>>,
    pub addr: String,
    pub path: PathBuf,
    pub node_id: NodeId,
}
impl Router {
    pub fn new(addr: String, path: PathBuf, node_id: NodeId) -> Self {
        Self {
            nodes: Arc::new((DashMap::new())),
            addr,
            path,
            node_id,
        }
    }
    // pub async fn register_node(&mut self, node_id: NodeId) {}
}
impl GroupRouter<TypeConfig, GroupId> for Router {
    //只有主节点会调用这个方法

    async fn append_entries(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        if !rpc.entries.is_empty() {
            let i = rpc.entries.len();
            tracing::info!("send entries len:{}", i);
        }
        let req = AppendEntriesReq {
            append_entries: rpc,
            group_id,
        };
        match self.nodes.get(&target) {
            None => {
                tracing::info!(
                    "node {} not found (target: {})",
                    target as u64,
                    target as u64
                );
                Err(RPCError::Unreachable(Unreachable::from_string(format!(
                    "node {} not found",
                    target as u64
                ))))
            }
            Some(client) => client.call(7, req).await,
        }
    }

    async fn vote(
        &self,
        target: NodeId,
        group_id: GroupId,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let req = VoteReq {
            vote: rpc,
            group_id,
        };
        match self.nodes.get(&target) {
            None => {
                tracing::info!(
                    "node {} not found (target: {})",
                    target as u64,
                    target as u64
                );
                Err(RPCError::Unreachable(Unreachable::from_string(format!(
                    "node {} not found",
                    target as u64
                ))))
            }
            Some(client) => client.call(6, req).await,
        }
    }

    //主节点给从节点发送快照
    async fn full_snapshot(
        &self,
        target: NodeId,
        group_id: GroupId,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // 如果节点不存在，返回 StreamingError
        let client = match self.nodes.get(&target) {
            None => {
                return Err(StreamingError::Unreachable(Unreachable::from_string(
                    format!("node {} not found", target as u64),
                )));
            }
            Some(c) => c,
        };

        let send_result = tokio::select! {
            _cancel_result = cancel => {
                //直接return 无需管返回值
                return Err(StreamingError::Timeout(Timeout{
                    action:InstallSnapshot,
                    target,
                    timeout:option.soft_ttl(),
                    id:self.node_id ,
                }));
            }
            send_result = snapshot.snapshot.send_file(&self.addr) => {
                send_result
            }
        };
        if send_result.is_err() {
            return Err(StreamingError::Unreachable(Unreachable::from_string(
                format!("node {} not found", target as u64),
            )));
        }

        let req = InstallFullSnapshotReq {
            vote,
            snapshot_meta: snapshot.meta,
            snapshot: snapshot.snapshot,
            group_id,
        };

        let result = client.call(8, req).await?;
        Ok(result)
    }
}
