use crate::raft::network::client::RpcMultiClient;
use crate::raft::network::model::{AppendEntriesReq, InstallFullSnapshotReq, VoteReq};
use crate::raft::types::file_operator::FileOperator;
use crate::raft::types::raft_types::{Node, NodeId, Snapshot, TypeConfig};
use crate::utils::now_ms;
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::RPCTypes::{InstallSnapshot, Vote};
use openraft::{OptionalSend, RaftNetworkFactory, RaftNetworkV2};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio_rustls::TlsConnector;
use tracing::info;

pub struct NetworkFactory {
    pub tls_connector: Option<TlsConnector>,
}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;
    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr = node.endpoint.raft_addr();
        TcpNetwork {
            tls_connector: self.tls_connector.clone(),
            addr: addr.clone(),
            nodes: Arc::new(RwLock::new(None)),
            target,
            node_id: node.node_id,
        }
    }
}

#[derive(Clone, Default)]
pub struct TcpNetwork {
    tls_connector: Option<TlsConnector>,
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

        match RpcMultiClient::connect(&self.addr, self.tls_connector.clone()).await {
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
    type SnapshotData = FileOperator;

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

    // 把已有的快照发送出去
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot,
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
        let tls_connector = self.tls_connector.clone();
        let hard_ttl = option.hard_ttl();
        // Phase 1: stream the file, phase 2: ask the follower to install it.
        // Both phases are cancellable: openraft closes the replication stream
        // when the leader steps down or the target is removed, and a leftover
        // install RPC would keep running against the follower otherwise.
        let transfer = async move {
            if let Err(e) = snapshot
                .snapshot
                .send_file(&client.addr, tls_connector)
                .await
            {
                info!("Failed to stream snapshot to node {}: {}", target, e);
                return Err(StreamingError::Unreachable(Unreachable::from_string(
                    format!(
                        "node {} not reachable for snapshot streaming",
                        target as u64
                    ),
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
                    hard_ttl,
                    Timeout {
                        action: InstallSnapshot,
                        target,
                        timeout: hard_ttl,
                        id: node_id,
                    },
                )
                .await?;
            Ok(result)
        };
        tokio::select! {
            closed = cancel => Err(StreamingError::Closed(closed)),
            result = transfer => result,
        }
    }
    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(Duration::from_millis(1500))))
    }
}
