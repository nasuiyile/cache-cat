use crate::raft::network::client::RpcMultiClient;
use crate::raft::network::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};
use crate::raft::types::raft_types::{Node, NodeId, TypeConfig};
use std::sync::Arc;
use std::time::Duration;

use crate::utils::now_ms;
use openraft::RPCTypes::{InstallSnapshot, Vote};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2, Snapshot};
use parking_lot::RwLock;
use tokio::time::sleep;
use tracing::info;

pub struct NetworkFactory {}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;
    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = node.endpoint.raft_addr();
        TcpNetwork {
            addr: addr.clone(),
            nodes: Arc::new(RwLock::new(None)),
            target,
            node_id: node.node_id,
        }
    }
}

#[derive(Clone, Default)]
pub struct TcpNetwork {
    addr: String,
    nodes: Arc<RwLock<Option<RpcMultiClient>>>,
    target: NodeId,
    node_id: NodeId,
}

impl TcpNetwork {
    // 辅助方法：获取客户端，如果不存在则尝试连接
    async fn get_or_connect_client(&self) -> Result<RpcMultiClient, RPCError<TypeConfig>> {
        // 先尝试读取现有的客户端
        {
            let guard = self.nodes.read();
            if let Some(client) = guard.as_ref() {
                return Ok(client.clone());
            }
        }

        // 如果不存在，尝试连接
        info!(
            "Attempting to connect to node {} at {}",
            self.target, self.addr
        );
        match RpcMultiClient::connect(&self.addr).await {
            Ok(client) => {
                let mut guard = self.nodes.write();
                // 双重检查，避免重复连接
                if guard.is_none() {
                    *guard = Some(client.clone());
                    info!(
                        "Successfully connected to node {} at {}",
                        self.target, self.addr
                    );
                }
                Ok(client)
            }
            Err(e) => {
                info!(
                    "Failed to connect to node {} at {}: {:?}",
                    self.target, self.addr, e
                );
                Err(RPCError::Unreachable(Unreachable::from_string(format!(
                    "node {} not reachable at {}",
                    self.target, self.addr
                ))))
            }
        }
    }
}

//openraft会自动调用这个方法，这里只需要实现网络层的rpc调用
impl RaftNetworkV2<TypeConfig> for TcpNetwork {
    //只有主节点会调用这个方法，主节点发起心跳时也会调用这个方法
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let req = AppendEntriesReq {
            append_entries: rpc,
        };

        let client = self.get_or_connect_client().await?;

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

        let client = self.get_or_connect_client().await?;

        let i = now_ms();
        let result = client
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
            .await;
        info!("调用方消耗时间{}", now_ms() - i);
        result
    }

    // 只是一个标识，并不真正进行快照
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let target = self.target;
        let node_id = self.node_id;

        let client = match self.get_or_connect_client().await {
            Ok(client) => client,
            Err(_) => {
                return Err(StreamingError::Unreachable(Unreachable::from_string(
                    format!("node {} not found", target as u64),
                )));
            }
        };

        let send_result = tokio::select! {
            _cancel_result = cancel => {
                //直接return 无需管返回值
                return Err(StreamingError::Timeout(Timeout{
                    action: InstallSnapshot,
                    target,
                    timeout: option.soft_ttl(),
                    id: node_id,
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
                    target,
                    timeout: option.hard_ttl(),
                    id: node_id,
                },
            )
            .await?;
        Ok(result)
    }
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(1500)))
    }
}
