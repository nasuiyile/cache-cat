use crate::server::handler::model::{SetReq, SetRes};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteReq {
    Set(SetReq),
}
impl WriteReq {
    pub fn set(key: impl Into<String>, value: impl Into<String>) -> Self {
        WriteReq::Set(SetReq {
            key: key.into(),
            value: Vec::from(value.into()),
            ex_time: 100000,
        })
    }
}

impl fmt::Display for WriteReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteReq::Set(req) => write!(f, "Set: {}", req),
        }
    }
}

pub type WriteRes = openraft::raft::ClientWriteResponse<crate::network::raft_rocksdb::TypeConfig>;

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteResRaft {
    Set(SetRes),
    Null,
}

impl WriteResRaft {
    pub fn none() -> Self {
        WriteResRaft::Null
    }
}
