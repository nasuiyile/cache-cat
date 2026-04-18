use crate::protocol::string::set::SetParams;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::raft_types::{GROUP_NUM, GroupId};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Base(BaseOperation),
    RedisSet(SetParams),
}

impl Request {
    pub fn get_group_id(&self) -> GroupId {
        let mut hasher = DefaultHasher::new();

        match self {
            Request::Base(op) => match op {
                BaseOperation::Set(req) => {
                    req.key.hash(&mut hasher);
                }
                BaseOperation::LPush(req) => {
                    req.key.hash(&mut hasher);
                }
                BaseOperation::Del(req) => {
                    if let Some(key) = req.keys.get(0) {
                        key.hash(&mut hasher);
                    } else {
                        return 0;
                    }
                }
            },

            Request::RedisSet(req) => {
                req.hash(&mut hasher);
            }
        }

        (hasher.finish() % GROUP_NUM as u64) as GroupId
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Base(op) => match op {
                BaseOperation::Set(req) => write!(f, "Set: {}", req),
                BaseOperation::LPush(req) => write!(f, "LPush: {}", req),
                BaseOperation::Del(req) => write!(f, "DEL: {}", req),
            },

            Request::RedisSet(req) => write!(f, "RedisSet: {}", req),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicRequest {
    pub request: BaseOperation,
    pub version: u32,
}
