use openraft::alias::VoteOf;
use openraft::{Snapshot, SnapshotMeta, raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
}};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use crate::network::raft_rocksdb::TypeConfig;
use bytes::Bytes;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PrintTestReq {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PrintTestRes {
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SetReq {
    pub key: String,
    pub value: Vec<u8>,
    pub ex_time: u64,
}
impl fmt::Display for SetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetReq {{ key: {}, value: {}, ex_time: {} }}",
            self.key,
            self.value.len(),
            self.ex_time
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SetRes {}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GetReq {
    pub key: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GetRes {
    // Arc<Vec<u8>> 在 serde 中有实现（在 std/alloc 可用的情况下）
    pub value: Option<Arc<Vec<u8>>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DelReq {
    pub key: String,
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InstallFullSnapshotReq {
    pub vote: VoteOf<TypeConfig>,
    pub snapshot_meta: SnapshotMeta<TypeConfig>,
    pub snapshot: Bytes,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct InstallFullSnapshotRes {
    pub value: SnapshotResponse<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug/*, PartialEq*/)]
pub struct AppendEntriesReq {
    pub data: AppendEntriesRequest<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct AppendEntriesRes {
    pub value: AppendEntriesResponse<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VoteReq {
    pub data: VoteRequest<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VoteRes {
    pub value: VoteResponse<TypeConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ReadReq {
    pub data: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ReadRes {
    pub value: Option<String>,
}