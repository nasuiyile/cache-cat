use super::endpoint::Endpoint;
use crate::raft::types::response_value::Value;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use crate::raft::types::entry::request::Request;

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
pub const GROUP_NUM: u16 = 1;

pub type GroupId = u16;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Value,
        NodeId = NodeId,
        Node = Node,
        SnapshotData = SnapshotData
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;

pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
