use super::endpoint::Endpoint;
use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::node::parsed_config::ParsedConfig;
use crate::raft::application::cluster::Cluster;
use crate::raft::application::connector::Connector;
use crate::raft::application::pub_sub::PubSub;
use crate::raft::store::statemachine::StateMachineStore;
use crate::raft::types::core::mocha::request_handler::read_request;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, Request};
use crate::raft::types::file_operator::FileOperator;
use futures::future::join_all;
use openraft::RPCTypes::Vote;
use openraft::error::Timeout;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

pub type SnapshotData = tokio::fs::File;

pub type NodeId = u16;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}", self.node_id, self.endpoint)
    }
}

pub type GroupId = u16;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Value,
        NodeId = NodeId,
        Node = Node,
        SnapshotData = FileOperator,
);

pub struct CacheCatApp {
    pub config: ParsedConfig,
    pub cluster: Cluster,
    pub state_machine: StateMachineStore,
    pub path: PathBuf,
    pub connector: Connector,
    pub pubsub: Arc<PubSub>,
    pub shutdown_tx: broadcast::Sender<()>,
}

impl CacheCatApp {
    pub async fn shutdown(&self) {
        _ = self.shutdown_tx.send(());
    }
    pub async fn leader_rpc_call<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
    ) -> Result<(), CacheCatError>
    where
        Req: Serialize + Clone + Send,
        Res: DeserializeOwned + Send,
    {
        if !self.cluster.is_leader() {
            return Err(ProtocolError::ReadOnly.into());
        }
        let nodes = self.cluster.nodes();
        let mut futures = Vec::new();
        for (node_id, node) in nodes {
            if node_id == self.cluster.node_id() {
                continue;
            }
            // 关键：将地址转为拥有所有权的 String，避免引用 node 造成生命周期问题
            let addr = node.endpoint.raft_addr().to_owned();
            let req = req.clone();
            let timeout = Timeout {
                action: Vote,
                target: node_id,
                timeout: Duration::from_secs(2),
                id: self.cluster.node_id(),
            };
            // 构造一个 Future 但不立即 .await
            let fut = self.connector.send_msg::<Req, Res>(
                addr, // 现在可以安全 move 进入 Future
                func_id,
                req,
                Duration::from_secs(2),
                timeout,
            );
            futures.push(fut);
        }
        // 并发执行所有请求，忽略结果
        let _ = join_all(futures).await;
        Ok(())
    }

    pub async fn write(&self, op: Operation, db_number: u16) -> Result<Value, CacheCatError> {
        let write_clock = self.state_machine.data.kvs.generate_new_write_clock();
        let request = Request::new(write_clock, db_number, op);
        let res = self
            .cluster
            .client_write(request)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        Ok(res.data)
    }
    pub async fn read(&self, param: ReadOperation, db_number: u16) -> Result<Value, CacheCatError> {
        self.cluster.lease_read().await?;
        let _read_lock = self.state_machine.data.kvs.read_lock.read();
        Ok(read_request(&self.state_machine.data.kvs, param, db_number))
    }

    pub async fn multi_read(
        &self,
        param: ReadOperation,
        db_number: u16,
    ) -> Result<Value, CacheCatError> {
        self.cluster.lease_read().await?;
        let _write_lock = self.state_machine.data.kvs.write_lock.lock().await;
        let _read_lock = self.state_machine.data.kvs.read_lock.read();
        Ok(read_request(&self.state_machine.data.kvs, param, db_number))
    }
}

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;

pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
pub type Raft = openraft::Raft<TypeConfig>;
