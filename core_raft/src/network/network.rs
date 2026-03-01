use crate::network::node::{NodeId, TypeConfig};
use crate::server::client::client::RpcMultiClient;
use crate::server::handler::model::{
    AppendEntriesReq, InstallFullSnapshotReq, PrintTestReq, PrintTestRes, VoteReq,
};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{
    BasicNode, OptionalSend, RaftNetworkFactory, RaftNetworkV2, RaftTypeConfig, Snapshot,
};
use openraft_multi::{GroupNetworkAdapter, GroupNetworkFactory};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Instant;

pub struct NetworkFactory {}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        //TODO 定时重连
        let client = RpcMultiClient::connect(&*node.addr.clone()).await.unwrap();
        TcpNetwork {
            addr: node.addr.clone(),
            client,
            target,
        }
    }
}

#[derive(Clone, Default)]
pub struct TcpNetwork {
    addr: String,
    client: RpcMultiClient,
    target: NodeId, //nodeid
}
impl TcpNetwork {
    async fn request<Req, Resp, Err>(
        &mut self,
        func_id: u32,
        req: Req,
    ) -> Result<Result<Resp, Err>, RPCError<TypeConfig>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let res: Result<Result<Resp, Err>, RPCError<TypeConfig>> =
            self.client.call(func_id, req).await.unwrap();
        res
    }
}

//openraft会自动调用这个方法，这里只需要实现网络层的rpc调用
impl RaftNetworkV2<TypeConfig> for TcpNetwork {
    //只有主节点会调用这个方法，朱姐带你发起心跳时也会调用这个方法
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let start = Instant::now();
        let is_heartbeat = rpc.entries.is_empty();
        let req = AppendEntriesReq {
            append_entries: rpc,
            group_id: 0,
        };
        let res: AppendEntriesResponse<TypeConfig> = self.client.call(7, req).await.unwrap();
        if is_heartbeat {
            tracing::info!(
                "append_entries 心跳 往返耗时: {} us",
                start.elapsed().as_micros()
            );
        } else {
            tracing::info!(
                "append_entries 条目 往返耗时: {} us",
                start.elapsed().as_micros()
            );
        }
        Ok(res)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let req = VoteReq {
            vote: rpc,
            group_id: 0,
        };
        let res: VoteResponse<TypeConfig> = self.client.call(6, req).await.unwrap_or_else(|e| {
            eprintln!("RPC call failed: {:?}", e);
            panic!("RPC call failed");
        });
        Ok(res)
    }
    // 只是一个标识，并不真正进行快照
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        mut snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let data = snapshot.snapshot.into_inner();
        let req = InstallFullSnapshotReq {
            vote,
            snapshot_meta: snapshot.meta,
            snapshot: data,
            group_id: 0,
        };
        self.client.call(8, req).await.unwrap()
    }
}
