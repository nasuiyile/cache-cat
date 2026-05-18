use crate::protocol::key::expire::ExpireCondition;
use crate::raft::types::core::value_object::ValueObject;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BaseOperation {
    // 只是用来推动时钟
    Empty,
    // key
    Del(DelReq),
    Expire(ExpireReq),
    Persist(PersistReq),
    Insert(InsertReq),
    //string
    Set(SetReq),
    Incr(IncrReq),
    Append(AppendReq),
    // list
    LPush(LPushReq),
    //hash
    HSet(HSetReq),
    HIncr(HIncrReq),
    HDel(HDelReq),
    // zset
    ZAdd(ZAddReq),
    // set
    SAdd(SAddReq),
    SRem(SRemReq),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SRemReq {
    pub key: Arc<Vec<u8>>,
    pub members: Vec<Arc<Vec<u8>>>,
}
impl fmt::Display for SRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SRemReq {{ key: {}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.members
        )
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HDelReq {
    pub key: Arc<Vec<u8>>,
    pub fields: Vec<Arc<Vec<u8>>>,
}
impl fmt::Display for HDelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HDelReq {{ key: {}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.fields
        )
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InsertReq {
    pub key: Arc<Vec<u8>>,
    pub value: ValueObject,
    pub expires_at: u64,
}
impl fmt::Display for InsertReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "InsertReq {{ key: {}, value: {:?}, expires_at: {} }}",
            String::from_utf8_lossy(&self.key),
            self.value,
            self.expires_at
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistReq {
    pub key: Arc<Vec<u8>>,
}
impl fmt::Display for PersistReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PersistReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HIncrReq {
    pub key: Arc<Vec<u8>>,
    pub field: Arc<Vec<u8>>,
    pub value: i64,
}
impl fmt::Display for HIncrReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HIncrReq {{ key: {}, field: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.field),
            self.value
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SAddReq {
    pub key: Arc<Vec<u8>>,
    pub elements: Vec<Arc<Vec<u8>>>,
}
impl fmt::Display for SAddReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SAddReq {{ key: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZAddReq {
    pub key: Arc<Vec<u8>>,
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub members: Vec<(Arc<Vec<u8>>, f64)>,
}
impl fmt::Display for ZAddReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZAddReq {{ key: {}, nx: {}, xx: {}, gt: {}, lt: {}, ch: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.nx,
            self.xx,
            self.gt,
            self.lt,
            self.ch,
            self.members
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HSetReq {
    pub key: Arc<Vec<u8>>,
    pub elements: Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>,
}
impl fmt::Display for HSetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HSetReq {{ key: {}, field: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppendReq {
    pub key: Arc<Vec<u8>>,
    pub value: Arc<Vec<u8>>,
}

impl fmt::Display for AppendReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AppendReq {{ key: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExpireReq {
    pub key: Arc<Vec<u8>>,
    pub expires_at: u64,
    pub condition: Option<ExpireCondition>,
}
impl fmt::Display for ExpireReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExpireReq {{ key: {}, seconds: {}, condition: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.expires_at,
            self.condition
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncrReq {
    pub key: Arc<Vec<u8>>,
    pub value: i64,
}
impl fmt::Display for IncrReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncrReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SetReq {
    pub key: Arc<Vec<u8>>,
    pub value: Arc<Vec<u8>>,
    pub ex_time: u64,
}
impl fmt::Display for SetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetReq {{ key: {}, value: {}, ex_time: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.ex_time
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LPushReq {
    pub key: Arc<Vec<u8>>,
    pub elements: Vec<Arc<Vec<u8>>>,
}
impl fmt::Display for LPushReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LPushReq {{ key: {}, elements: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DelReq {
    pub key: Arc<Vec<u8>>,
}
impl fmt::Display for DelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DelReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
