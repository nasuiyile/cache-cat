use crate::protocol::bitmap::setbit::SetBitReq;
use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::key::del::DelReq;
use crate::protocol::key::persist::PersistReq;
use crate::protocol::key::pexpire::PExpireReq;
use crate::protocol::list::lpop::LPopReq;
use crate::protocol::list::lpush::LPushReq;
use crate::protocol::list::lrem::LRemReq;
use crate::protocol::list::lset::LSetReq;
use crate::protocol::list::rpop::RPopReq;
use crate::protocol::list::rpush::RPushReq;
use crate::protocol::set::sadd::SAddReq;
use crate::protocol::set::srem::SRemReq;
use crate::protocol::string::append::AppendReq;
use crate::protocol::string::incr::IncrReq;
use crate::protocol::string::set::SetReq;
use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::value_object::ValueObject;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BaseOperation {
    // Just used to push the clock
    Empty,
    // key
    Del(DelReq),
    PExpire(PExpireReq),
    Persist(PersistReq),
    Insert(InsertReq),
    //string
    Set(SetReq),
    Incr(IncrReq),
    Append(AppendReq),
    SetBit(SetBitReq),
    // list
    LPush(LPushReq),
    RPush(RPushReq),
    LPop(LPopReq),
    RPop(RPopReq),
    LRem(LRemReq),
    LSet(LSetReq),
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
pub struct InsertReq {
    pub key: Bytes,
    pub value: ValueObject,
    pub expires_at: u64,
}

impl Display for InsertReq {
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
