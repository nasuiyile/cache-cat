use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::file_operator::FileOperator;
use crate::raft::types::raft_types::{GroupId, Node, NodeId, TypeConfig};
use openraft::SnapshotMeta;
use openraft::alias::VoteOf;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hasher;
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PrintTestReq {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PrintTestRes {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GetReq {
    pub key: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GetRes {
    // Arc<Vec<u8>> 在 serde 中有实现（在 std/alloc 可用的情况下）
    pub value: Option<Arc<Vec<u8>>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DelRes {
    pub num: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ExistsReq {
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ExistsRes {
    pub num: u32,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesReq {
    pub append_entries: AppendEntriesRequest<TypeConfig>,
    pub group_id: GroupId,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteReq {
    pub vote: VoteRequest<TypeConfig>,
    pub group_id: GroupId,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InstallFullSnapshotReq {
    pub vote: VoteOf<TypeConfig>,
    pub snapshot_meta: SnapshotMeta<TypeConfig>,
    pub snapshot: FileOperator,
    pub group_id: GroupId,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct AddNodeReq {
    pub node: Node,
}
