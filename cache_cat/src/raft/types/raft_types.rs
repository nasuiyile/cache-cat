use super::endpoint::Endpoint;
use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::raft::network::connector::Connector;
use crate::raft::network::pub_sub::PubSub;
use crate::raft::store::statemachine::StateMachineStore;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, Request};
use crate::raft::types::file_operator::FileOperator;
use openraft::RPCTypes::Vote;
use openraft::ReadPolicy::LeaseRead;
use openraft::async_runtime::WatchReceiver;
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
    pub node_id: NodeId,
    pub raft: Raft,
    pub state_machine: StateMachineStore,
    pub path: PathBuf,
    pub connector: Connector,
    pub broadcast: Arc<PubSub>,
}

impl CacheCatApp {
    pub async fn leader_rpc_call<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
    ) -> Result<(), CacheCatError>
    where
        Req: Serialize + Clone + Send,
        Res: DeserializeOwned + Send,
    {
        if !self.raft.is_leader() {
            return Err(ProtocolError::ReadOnly.into());
        }
        // 用作用域确保所有 guard 提前释放
        let nodes = {
            let metrics_guard = self.raft.metrics();
            let metrics = metrics_guard.borrow_watched();
            metrics
                .membership_config
                .nodes()
                .map(|(node_id, node)| (*node_id, node.clone()))
                .collect::<Vec<_>>()
        };
        for (node_id, node) in nodes {
            if node_id == self.node_id {
                continue;
            }
            let timeout = Timeout {
                action: Vote,
                target: node_id,
                timeout: Duration::from_secs(5),
                id: self.node_id,
            };
            self.connector
                .send_msg::<Req, Res>(
                    &node.endpoint.to_string(),
                    func_id,
                    req.clone(),
                    Duration::from_secs(5),
                    timeout,
                )
                .await?;
        }

        Ok(())
    }

    pub async fn write(&self, op: Operation, db_number: u16) -> Result<Value, CacheCatError> {
        let write_clock = self.state_machine.data.kvs.generate_new_write_clock();
        let request = Request::new(write_clock, db_number, op);
        let res = self
            .raft
            .client_write(request)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        Ok(res.data)
    }
    pub async fn read(
        &self,
        key: Vec<u8>,
        db_number: u16,
    ) -> Result<Option<MyValue>, CacheCatError> {
        let linearizer = self
            .raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&self.raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        let read_lock = self.state_machine.data.kvs.read_lock.read();
        let my_value = self
            .state_machine
            .data
            .kvs
            .get_value_with_read_clock(&key, db_number)?;
        drop(read_lock);
        Ok(my_value)
    }

    pub async fn multi_read(
        &self,
        keys: Vec<Vec<u8>>,
        db_number: u16,
    ) -> Result<Vec<Option<MyValue>>, CacheCatError> {
        let linearizer = self
            .raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&self.raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        let _write_lock = self.state_machine.data.kvs.write_lock.lock().await;
        let _read_lock = self.state_machine.data.kvs.read_lock.read();

        let mut vec = Vec::new();
        for key in keys {
            let my_value = self
                .state_machine
                .data
                .kvs
                .get_value_with_read_clock(&key, db_number)?;
            vec.push(my_value);
        }
        Ok(vec)
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
