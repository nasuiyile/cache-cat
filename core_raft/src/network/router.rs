use crate::network::node::{GroupId, NodeId, TypeConfig};
use crate::server::client::client::{RpcClient, RpcMultiClient};
use crate::server::handler::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};

use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkFactory, Snapshot};
use openraft_multi::{GroupNetworkAdapter, GroupNetworkFactory, GroupRouter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

pub type MultiNetworkFactory = GroupNetworkFactory<Router, GroupId>;
impl RaftNetworkFactory<TypeConfig> for MultiNetworkFactory {
    type Network = GroupNetworkAdapter<TypeConfig, GroupId, Router>;

    //实际创建连接
    //TODO 定时重连
    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        let mut router = self.factory.clone();
        match RpcMultiClient::connect(&*node.addr).await {
            Ok(client) => {
                router.nodes.insert(target, client);
            }
            Err(_) => {
                tracing::info!("connect to node {} failed", node.addr)
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
    pub nodes: Box<HashMap<NodeId, RpcMultiClient>>,
    pub addr: String,
}
impl Router {
    pub fn new(addr: String) -> Self {
        Self {
            nodes: Box::new((HashMap::new())),
            addr,
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
            Some(client) => {
                // ----------- 统计开始 -----------
                let start = Instant::now();

                let result = client.call(7, req).await;

                let elapsed = start.elapsed();
                tracing::info!(
                    "RPC call slave to node {} took {:?}",
                    target as u64,
                    elapsed
                );
                // ----------- 统计结束 -----------

                match result {
                    Ok(r) => Ok(r),
                    Err(e) => {
                        tracing::info!("RPC call to node {} failed: {}", target as u64, e);
                        Err(RPCError::Unreachable(Unreachable::from_string(format!(
                            "RPC call to node {} failed: {}",
                            target as u64, e
                        ))))
                    }
                }
            }
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
            Some(client) => match client.call(6, req).await {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::info!("RPC call to node {} failed: {}", target as u64, e);
                    let unreachable = Unreachable::from_string(format!(
                        "RPC call to node {} failed: {}",
                        target as u64, e
                    ));
                    Err(RPCError::Unreachable(unreachable))
                }
            },
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
        let data = snapshot.snapshot.into_inner();
        let req = InstallFullSnapshotReq {
            vote,
            snapshot_meta: snapshot.meta,
            snapshot: data,
            group_id,
        };
        self.nodes.get(&target).unwrap().call(8, req).await.unwrap()
    }
}
