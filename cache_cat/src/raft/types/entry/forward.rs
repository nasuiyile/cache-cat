use crate::raft::types::entry::membership::JoinRequest;
use crate::raft::types::raft_types::TypeConfig;
use openraft::EntryPayload;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ForwardRequestBody {
    Join(JoinRequest),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForwardRequest {
    pub forward_to_leader: u64,
    pub body: ForwardRequestBody,
}
