use crate::protocol::key::del::DelParams;
use crate::protocol::key::rename::RenameParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::SetParams;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::utils::merge_u64;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Base(u64, BaseOperation),
    Redis(u64, RedisOperation),
}
impl Request {
    #[inline]
    pub fn new_base(write_clock: u64, db_number: u16, request: BaseOperation) -> Self {
        Request::Base(merge_u64(write_clock, db_number), request)
    }

    #[inline]
    pub fn new_redis(write_clock: u64, db_number: u16, request: RedisOperation) -> Self {
        Request::Redis(merge_u64(write_clock, db_number), request)
    }

    #[inline]
    pub fn set_write_clock(&mut self, high_bits: u64) {
        let masked = high_bits << 16; // 高48位移到高位
        match self {
            Request::Base(val, _) | Request::Redis(val, _) => {
                *val = (*val & 0xFFFF) | (masked & 0xFFFFFFFFFFFF0000);
            }
        }
    }
    #[inline]
    pub fn split_u64(&self) -> (u64, u16) {
        let value = match self {
            Request::Base(value, _) => value,
            Request::Redis(value, _) => value,
        };
        let high_48: u64 = value >> 16; // 取高 48 位 作为当前时间戳毫秒值
        let low_16: u16 = (value & 0xFFFF) as u16; // 取低 16 位 作为数据库编号
        (high_48, low_16)
    }
    #[inline]
    pub fn get_db_number(&self) -> u16 {
        let value = match self {
            Request::Base(value, _) => value,
            Request::Redis(value, _) => value,
        };
        (value >> 16) as u16
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisOperation {
    RedisSet(SetParams),
    RedisMset(MsetParams),
    RedisDel(DelParams),
    RedisRename(RenameParams),
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Base(.., op) => match op {
                BaseOperation::Empty => write!(f, "None"),
                BaseOperation::Set(req) => write!(f, "Set: {}", req),
                BaseOperation::LPush(req) => write!(f, "LPush: {}", req),
                BaseOperation::Del(req) => write!(f, "DEL: {}", req),
                BaseOperation::Incr(req) => write!(f, "Incr: {}", req),
                BaseOperation::Expire(req) => write!(f, "Expire: {}", req),
                BaseOperation::Append(req) => write!(f, "Append: {}", req),
                BaseOperation::HSet(req) => write!(f, "HSet: {}", req),
                BaseOperation::ZAdd(req) => write!(f, "ZAdd: {}", req),
                BaseOperation::SAdd(req) => write!(f, "SAdd: {}", req),
                BaseOperation::HIncr(req) => write!(f, "HIncr: {}", req),
                BaseOperation::Persist(req) => write!(f, "Persist: {}", req),
                BaseOperation::Insert(insert) => write!(f, "Insert: {}", insert),
            },
            Request::Redis(.., redis) => match redis {
                RedisOperation::RedisSet(req) => write!(f, "RedisSet: {}", req),
                RedisOperation::RedisMset(req) => write!(f, "RedisMset: {}", req),
                RedisOperation::RedisDel(req) => write!(f, "RedisDel: {}", req),
                RedisOperation::RedisRename(req) => write!(f, "RedisRename: {}", req),
            },
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicRequest {
    pub request: BaseOperation,
    pub version: u32,
    pub write_clock: u64,
}
