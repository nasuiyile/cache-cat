use crate::protocol::bitmap::setbit::SetBitReq;
use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::hash::hsetnx::HSetNxReq;
use crate::protocol::key::del::DelReq;
use crate::protocol::key::expire::ExpireReq;
use crate::protocol::key::flushdb::FlushDBReq;
use crate::protocol::key::persist::PersistReq;
use crate::protocol::key::pexpire::PExpireReq;
use crate::protocol::list::lpop::LPopReq;
use crate::protocol::list::lpush::LPushReq;
use crate::protocol::list::lrem::LRemReq;
use crate::protocol::list::lset::LSetReq;
use crate::protocol::list::ltrim::LTrimReq;
use crate::protocol::list::rpop::RPopReq;
use crate::protocol::list::rpush::RPushReq;
use crate::protocol::set::sadd::SAddReq;
use crate::protocol::set::srem::SRemReq;
use crate::protocol::string::append::AppendReq;
use crate::protocol::string::decr::DecrReq;
use crate::protocol::string::decrby::DecrByReq;
use crate::protocol::string::incr::IncrReq;
use crate::protocol::string::incrby::IncrByReq;
use crate::protocol::string::set::SetReq;
use crate::protocol::zset::zadd::ZAddReq;
use crate::protocol::zset::zrem::ZRemReq;
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
    Expire(ExpireReq),
    PExpire(PExpireReq),
    Persist(PersistReq),
    Insert(InsertReq),
    FlushDB(FlushDBReq),
    //string
    Set(SetReq),
    Incr(IncrReq),
    IncrBy(IncrByReq),
    Append(AppendReq),
    SetBit(SetBitReq),
    DecrBy(DecrByReq),
    Decr(DecrReq),
    // list
    LPush(LPushReq),
    RPush(RPushReq),
    LPop(LPopReq),
    RPop(RPopReq),
    LRem(LRemReq),
    LSet(LSetReq),
    LTrim(LTrimReq),
    //hash
    HSet(HSetReq),
    HIncr(HIncrReq),
    HDel(HDelReq),
    HSetNx(HSetNxReq),
    // zset
    ZAdd(ZAddReq),
    ZRem(ZRemReq),
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
