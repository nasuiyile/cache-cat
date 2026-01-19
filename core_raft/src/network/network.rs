use crate::network::raft::TypeConfig;
use client::client::RpcClient;
use fory_core::{ForyDefault, Serializer};
use fory_derive::ForyObject;
use openraft::alias::VoteOf;
use openraft::entry::RaftPayload;
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
use std::fmt::Display;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
pub struct NetworkFactory {}
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = TcpNetwork;
    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let client = RpcClient::connect("127.0.0.1:8080").await.unwrap();
        TcpNetwork {
            addr: node.addr.clone(),
            client,
            target,
        }
    }
}

pub struct TcpNetwork {
    addr: String,
    client: RpcClient,
    target: u64, //nodeid
}
impl TcpNetwork {
    async fn request<Req, Resp, Err>(
        &mut self,
        func_id: u32,
        req: Req,
    ) -> Result<Result<Resp, Err>, RPCError<TypeConfig>>
    where
        Req: Serializer + ForyDefault,
        Resp: Serializer + ForyDefault,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        // let res = (self.client).call(func_id, req).await.unwrap();
        // Ok(res)
        todo!()
    }
}

impl RaftNetworkV2<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let a = rpc.vote.leader_id.node_id;
        let b = rpc.vote.leader_id.term;
        let c = rpc.vote.committed;
        let vec = rpc.entries;
        todo!()
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        todo!()
    }
    // 只是一个标识，并不真正进行快照
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        todo!()
    }
}
