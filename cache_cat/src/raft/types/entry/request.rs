use crate::protocol::key::rename::RenameParams;
use crate::protocol::key::renamenx::RenameNxParams;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::string::getset::GetSetParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::SetParams;
use crate::protocol::string::setex::SetExParams;
use crate::protocol::string::setnx::SetNxParams;
use crate::protocol::transaction::exec::ExecParams;
use crate::protocol::{key::del::DelParams, string::psetex::PSetExParams};
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::merge_u64;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Base(BaseOperation),
    Read(ReadOperation),
    Redis(RedisOperation),
}

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub number: u64,
    pub operation: Operation,
}
impl Request {
    #[inline]
    pub fn new(write_clock: u64, db_number: u16, operation: Operation) -> Self {
        Request {
            number: merge_u64(write_clock, db_number),
            operation,
        }
    }

    #[inline]
    pub fn set_write_clock(&mut self, high_bits: u64) {
        let masked = high_bits << 16; // 48 high displacement to high position
        self.number = (self.number & 0xFFFF) | (masked & 0xFFFFFFFFFFFF0000);
    }

    #[inline]
    pub fn split_u64(&self) -> (u64, u16) {
        let high_48: u64 = self.number >> 16; // Take the highest 48 bits as the current timestamp value in milliseconds
        let low_16: u16 = (self.number & 0xFFFF) as u16; // Take the lower 16 digits as the database number
        (high_48, low_16)
    }

    #[inline]
    pub fn get_db_number(&self) -> u16 {
        (self.number >> 16) as u16
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisOperation {
    RedisSet(SetParams),
    RedisSetNx(SetNxParams),
    RedisSetEx(SetExParams),
    RedisPSetEx(PSetExParams),
    RedisGetSet(GetSetParams),
    RedisMset(MsetParams),
    RedisDel(DelParams),
    RedisRename(RenameParams),
    RedisRenameNx(RenameNxParams),
    RedisEval(EvalParams),
    RedisExec(ExecParams),
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.operation {
            Operation::Read(op) => match op {
                ReadOperation::Get(req) => write!(f, "Get: {}", req),
                ReadOperation::MGet(req) => write!(f, "MGet: {}", req),
                ReadOperation::ZRange(req) => write!(f, "ZRange: {}", req),
                ReadOperation::Exists(req) => write!(f, "Exists: {}", req),
                ReadOperation::LRange(req) => write!(f, "LRange: {}", req),
                ReadOperation::HGet(req) => write!(f, "HGet: {}", req),
                ReadOperation::SMembers(req) => write!(f, "SMembers: {}", req),
                ReadOperation::HMGet(req) => write!(f, "HMGet: {}", req),
                ReadOperation::GetBit(req) => write!(f, "GetBit: {}", req),
                ReadOperation::ZRangeByScore(req) => write!(f, "ZRangeByScore: {}", req),
                ReadOperation::StrLen(req) => write!(f, "StrLen: {}", req),
                ReadOperation::HGetAll(req) => write!(f, "HGetAll: {}", req),
                ReadOperation::HKeys(req) => write!(f, "HKeys: {}", req),
                ReadOperation::HVals(req) => write!(f, "HVals: {}", req),
                ReadOperation::LLen(req) => write!(f, "HVals: {}", req),
                ReadOperation::Type(req) => write!(f, "Type: {}", req),
                ReadOperation::LIndex(req) => write!(f, "LIndex: {}", req),
                ReadOperation::SIsMember(req) => write!(f, "SIsMember: {}", req),
                ReadOperation::HExists(req) => write!(f, "HExists: {}", req),
                ReadOperation::PTtl(req) => write!(f, "Pttl: {}", req),
                ReadOperation::Ttl(req) => write!(f, "Ttl: {}", req),
                ReadOperation::HLen(req) => write!(f, "HLen: {}", req),
            },
            Operation::Base(op) => match op {
                BaseOperation::Empty => write!(f, "None"),
                BaseOperation::Set(req) => write!(f, "Set: {}", req),
                BaseOperation::LPush(req) => write!(f, "LPush: {}", req),
                BaseOperation::Del(req) => write!(f, "DEL: {}", req),
                BaseOperation::Incr(req) => write!(f, "Incr: {}", req),
                BaseOperation::IncrBy(req) => write!(f, "IncrBy: {}", req),
                BaseOperation::Expire(req) => write!(f, "Expire: {}", req),
                BaseOperation::PExpire(req) => write!(f, "PExpire: {}", req),
                BaseOperation::Append(req) => write!(f, "Append: {}", req),
                BaseOperation::HSet(req) => write!(f, "HSet: {}", req),
                BaseOperation::ZAdd(req) => write!(f, "ZAdd: {}", req),
                BaseOperation::SAdd(req) => write!(f, "SAdd: {}", req),
                BaseOperation::HIncr(req) => write!(f, "HIncr: {}", req),
                BaseOperation::Persist(req) => write!(f, "Persist: {}", req),
                BaseOperation::Insert(insert) => write!(f, "Insert: {}", insert),
                BaseOperation::HDel(req) => write!(f, "HDel: {}", req),
                BaseOperation::SRem(req) => write!(f, "SRem: {}", req),
                BaseOperation::SetBit(req) => write!(f, "SetBit: {}", req),
                BaseOperation::LPop(req) => write!(f, "LPop: {}", req),
                BaseOperation::RPush(req) => write!(f, "RPush: {}", req),
                BaseOperation::RPop(req) => write!(f, "RPop: {}", req),
                BaseOperation::LRem(req) => write!(f, "LRem: {}", req),
                BaseOperation::LSet(req) => write!(f, "LSet: {}", req),
                BaseOperation::DecrBy(req) => write!(f, "DecrBy: {}", req),
                BaseOperation::HSetNx(req) => write!(f, "HSetNx: {}", req),
                BaseOperation::Decr(req) => write!(f, "Decr: {}", req),
                BaseOperation::ZRem(req) => write!(f, "ZRem: {}", req),
                BaseOperation::LTrim(req) => write!(f, "LTrim: {}", req),
                BaseOperation::FlushDB(req) => write!(f, "FlushDB: {}", req),
            },
            Operation::Redis(op) => match op {
                RedisOperation::RedisSet(req) => write!(f, "RedisSet: {}", req),
                RedisOperation::RedisSetNx(req) => write!(f, "RedisSetNx: {}", req),
                RedisOperation::RedisSetEx(req) => write!(f, "RedisSetEx: {}", req),
                RedisOperation::RedisPSetEx(req) => write!(f, "RedisPSetEx: {}", req),
                RedisOperation::RedisMset(req) => write!(f, "RedisMset: {}", req),
                RedisOperation::RedisDel(req) => write!(f, "RedisDel: {}", req),
                RedisOperation::RedisRename(req) => write!(f, "RedisRename: {}", req),
                RedisOperation::RedisEval(req) => write!(f, "RedisEval: {}", req),
                RedisOperation::RedisExec(req) => write!(f, "RedisExec: {}", req),
                RedisOperation::RedisRenameNx(req) => write!(f, "RedisRenameNx: {}", req),
                RedisOperation::RedisGetSet(req) => write!(f, "RedisGetSet: {}", req),
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
