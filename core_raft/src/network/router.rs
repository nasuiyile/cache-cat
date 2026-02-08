use crate::server::client::client::RpcMultiClient;
use crate::server::handler::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkFactory, Snapshot};
use openraft_multi::{GroupNetworkAdapter, GroupNetworkFactory, GroupRouter};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::network::node::{GroupId, NodeId, TypeConfig};

pub type MultiNetworkFactory = GroupNetworkFactory<Router, GroupId>;
impl RaftNetworkFactory<TypeConfig> for MultiNetworkFactory {
    type Network = GroupNetworkAdapter<TypeConfig, GroupId, Router>;

    //实际创建连接
    //TODO 定时重连
    async fn new_client(&mut self, target: NodeId, node: &openraft::BasicNode) -> Self::Network {
        let router = self.factory.clone();
        let client = RpcMultiClient::connect(&*node.addr).await.unwrap();
        router.nodes.write().await.insert(target, client);
        GroupNetworkAdapter::new(router, target, self.group_id.clone())
    }
}

/// Multi-Raft Router with per-node connection sharing.
#[derive(Clone, Default)]
pub struct Router {
    /// Map from node_id to node connection.
    /// 所有节点都有这个nodes副本
    pub nodes: Arc<RwLock<HashMap<NodeId, RpcMultiClient>>>,
    pub addr: String,
}
impl Router {
    pub fn new(addr: String) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
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
        let req = AppendEntriesReq {
            append_entries: rpc,
            group_id,
        };
        let res = self
            .nodes
            .read()
            .await
            .get(&target)
            .unwrap()
            .call(7, req)
            .await
            .unwrap();
        Ok(res)
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
        let res = self
            .nodes
            .read()
            .await
            .get(&target)
            .unwrap()
            .call(6, req)
            .await
            .unwrap();
        Ok(res)
    }

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
        self.nodes
            .read()
            .await
            .get(&target)
            .unwrap()
            .call(8, req)
            .await
            .unwrap()
    }
}
