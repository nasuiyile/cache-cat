use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BaseOperation {
    Set(SetReq),
    LPush(LPushReq),
    Del(DelReq),
    Incr(IncrReq),
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct LPushReq {
    pub key: Arc<Vec<u8>>,
    pub value: Arc<Vec<u8>>,
}
impl fmt::Display for LPushReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LPushReq {{ key: {}, value: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DelReq {
    pub keys: Arc<Vec<Vec<u8>>>,
}
impl fmt::Display for DelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DelReq {{ keys: {:?} }}", self.keys)
    }
}
