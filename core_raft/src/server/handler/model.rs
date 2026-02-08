use openraft::alias::VoteOf;
use openraft::raft::{AppendEntriesRequest, VoteRequest};
use openraft::{Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::sync::Arc;
use crate::network::node::{GroupId, TypeConfig};

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
impl Hash for SetReq {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
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
    pub snapshot: Vec<u8>,
    pub group_id: GroupId,
}
