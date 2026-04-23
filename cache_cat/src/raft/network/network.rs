use crate::raft::network::client::RpcMultiClient;
use crate::raft::network::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};
use crate::raft::types::raft_types::{Node, NodeId, TypeConfig};
use std::sync::Arc;
use std::time::Duration;

use openraft::RPCTypes::{InstallSnapshot, Vote};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2, Snapshot};
use parking_lot::RwLock;
use tokio::time::sleep;

pub struct NetworkFactory {}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;
    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = node.endpoint.to_string();
        let nodes = Arc::new(RwLock::new(None));
        let arc_nodes = nodes.clone();
        match RpcMultiClient::connect(&*node.endpoint.to_string()).await {
            Ok(client) => {
                _ = arc_nodes.write().insert(client);
            }
            Err(_) => {
                tracing::info!("connect to node {} failed, start retrying", addr);
                tokio::spawn(async move {
                    loop {
                        sleep(Duration::from_secs(2)).await;
                        match RpcMultiClient::connect(&addr).await {
                            Ok(client) => {
                                tracing::info!("reconnect to {} success", addr);
                                _ = arc_nodes.write().insert(client);
                                break; // 成功后退出循环
                            }
                            Err(_) => {
                                tracing::debug!("retry connect to {} failed", addr);
                            }
                        }
                    }
                });
            }
        };
        TcpNetwork {
            nodes: nodes,
            target,
            node_id: node.node_id,
        }
    }
}

#[derive(Clone, Default)]
pub struct TcpNetwork {
    pub nodes: Arc<RwLock<Option<RpcMultiClient>>>,
    // client: RpcMultiClient,
    target: NodeId,
    node_id: NodeId,
}

//openraft会自动调用这个方法，这里只需要实现网络层的rpc调用
impl RaftNetworkV2<TypeConfig> for TcpNetwork {
    //只有主节点会调用这个方法，朱姐带你发起心跳时也会调用这个方法
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let req = AppendEntriesReq {
            append_entries: rpc,
        };
        let client = {
            match &*self.nodes.read() {
                None => {
                    return Err(RPCError::Unreachable(Unreachable::from_string(format!(
                        "node {} not found",
                        self.target as u64
                    ))));
                }
                Some(client) => client.clone(),
            }
        };
        client
            .call_with_timeout(
                7,
                req,
                option.hard_ttl(),
                Timeout {
                    action: Vote,
                    target: self.target,
                    timeout: option.hard_ttl(),
                    id: self.node_id,
                },
            )
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let req = VoteReq { vote: rpc };
        let client = {
            match &*self.nodes.read() {
                None => {
                    return Err(RPCError::Unreachable(Unreachable::from_string(format!(
                        "node {} not found",
                        self.target as u64
                    ))));
                }
                Some(client) => client.clone(),
            }
        };
        client
            .call_with_timeout(
                6,
                req,
                option.hard_ttl(),
                Timeout {
                    action: Vote,
                    target: self.target,
                    timeout: option.hard_ttl(),
                    id: self.node_id,
                },
            )
            .await
    }
    // 只是一个标识，并不真正进行快照
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        mut snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let target = self.target;
        let node_id = self.node_id;
        let client = {
            let guard = self.nodes.read();
            match &*guard {
                None => {
                    return Err(StreamingError::Unreachable(Unreachable::from_string(
                        format!("node {} not found", target as u64),
                    )));
                }
                Some(c) => c.clone(), // ⭐ 必须 clone
            }
        };

        let send_result = tokio::select! {
            _cancel_result = cancel => {
                //直接return 无需管返回值
                return Err(StreamingError::Timeout(Timeout{
                    action:InstallSnapshot,
                    target,
                    timeout:option.soft_ttl(),
                    id:node_id ,
                }));
            }
            send_result = snapshot.snapshot.send_file(&*client.addr) => {
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
        };

        let result = client
            .call_with_timeout(
                8,
                req,
                option.hard_ttl(),
                Timeout {
                    action: Vote,
                    target: target,
                    timeout: option.hard_ttl(),
                    id: node_id,
                },
            )
            .await?;
        Ok(result)
    }
}
