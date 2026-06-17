use crate::raft::types::core::response_value::Value;
use crate::raft::types::file_operator::FileOperator;
use crate::raft::types::raft_types::{Node, TypeConfig};
use bytes::Bytes;
use openraft::SnapshotMeta;
use openraft::alias::VoteOf;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use serde::{Deserialize, Serialize};
use std::hash::Hasher;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct PublishReq {
    pub message: Bytes,
    pub channel: Bytes,
}

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
    pub db_number: u16,
    pub key: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetRes {
    // Arc<Vec<u8>> 在 serde 中有实现（在 std/alloc 可用的情况下）
    pub value: Value,
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
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteReq {
    pub vote: VoteRequest<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InstallFullSnapshotReq {
    pub vote: VoteOf<TypeConfig>,
    pub snapshot_meta: SnapshotMeta<TypeConfig>,
    pub snapshot: FileOperator,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct AddNodeReq {
    pub node: Node,
}
