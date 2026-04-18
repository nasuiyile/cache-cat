use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::raft_types::{Node, NodeId};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct GetMembersReq {}

pub type GetMembersReply = BTreeMap<u64, Node>;


#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
    pub node_id: NodeId,
}
