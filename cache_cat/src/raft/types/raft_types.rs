use super::endpoint::Endpoint;
use crate::raft::store::statemachine::StateMachineStore;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Request;
use crate::raft::types::file_operator::FileOperator;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

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
        SnapshotData = FileOperator,
);

pub struct CacheCatApp {
    pub node_id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub group_id: GroupId,
    pub state_machine: StateMachineStore,
    pub path: PathBuf,
}

// app 初始化后就不会变了
pub type App = Arc<Vec<Arc<CacheCatApp>>>;
pub fn get_app(app: &App, group_id: GroupId) -> &CacheCatApp {
    app.iter().find(|app| app.group_id == group_id).unwrap()
}
pub fn get_group(app: &App, hash_code: u64) -> &CacheCatApp {
    let usize = hash_code % app.len() as u64;
    get_app(app, usize as GroupId)
}
pub fn get_group_by_key<'a>(app: &'a App, key: &Vec<u8>) -> &'a CacheCatApp {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    get_group(app, hasher.finish())
}
pub fn get_group_id_by_key(key: &Vec<u8>) -> GroupId {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % GROUP_NUM as u64) as GroupId
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
