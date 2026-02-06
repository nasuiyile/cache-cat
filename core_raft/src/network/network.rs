use crate::error::CoreRaftResult;
use crate::network::raft_rocksdb::TypeConfig;
use crate::server::client::client::RpcMultiClient;
use crate::server::handler::model::{InstallFullSnapshotReq, PrintTestReq, PrintTestRes};
use openraft::alias::VoteOf;
use openraft::error::{RPCError, ReplicationClosed, StreamingError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{
    BasicNode, OptionalSend, RaftNetworkFactory, RaftNetworkV2, RaftTypeConfig, Snapshot,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Instant;

pub struct NetworkFactory {}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;

    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let client = RpcMultiClient::connect(&*node.addr.clone(), 5)
            .await
            .unwrap();
        TcpNetwork {
            addr: node.addr.clone(),
            client,
            target,
        }
    }
}
pub struct TcpNetwork {
    addr: String,
    client: RpcMultiClient,
    target: u64, //nodeid
}
impl TcpNetwork {
    async fn request<Req, Resp>(&mut self, func_id: u32, req: Req) -> CoreRaftResult<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        //Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let res = self.client.call(func_id, req).await.unwrap();
        Ok(res)
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
        let res: AppendEntriesResponse<TypeConfig> = self.client.call(7, rpc).await.unwrap();
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
        let res: VoteResponse<TypeConfig> = self.client.call(6, rpc).await.unwrap_or_else(|e| {
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
        let data = snapshot.snapshot.clone();
        let req = InstallFullSnapshotReq {
            vote,
            snapshot_meta: snapshot.meta,
            snapshot: data,
        };
        self.client.call(8, req).await.unwrap()
    }
}
