use crate::protocol::key::del::DelParams;
use crate::protocol::key::rename::RenameParams;
use crate::protocol::list::blpop::BLPopParams;
use crate::protocol::lua::eval::EvalParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::SetParams;
use crate::protocol::transaction::exec::ExecParams;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::utils::merge_u64;
use serde::{Deserialize, Serialize};
use std::fmt;
use crate::protocol::key::renamenx::RenameNxParams;

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
        let masked = high_bits << 16; // 高48位移到高位
        self.number = (self.number & 0xFFFF) | (masked & 0xFFFFFFFFFFFF0000);
    }

    #[inline]
    pub fn split_u64(&self) -> (u64, u16) {
        let high_48: u64 = self.number >> 16; // 取高 48 位作为当前时间戳毫秒值
        let low_16: u16 = (self.number & 0xFFFF) as u16; // 取低 16 位作为数据库编号
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
    RedisMset(MsetParams),
    RedisDel(DelParams),
    RedisRename(RenameParams),
    RedisRenameNx(RenameNxParams),
    RedisEval(EvalParams),
    RedisExec(ExecParams),
    RedisBLPop(BLPopParams),
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
            },
            Operation::Base(op) => match op {
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
                BaseOperation::HDel(req) => write!(f, "HDel: {}", req),
                BaseOperation::SRem(req) => write!(f, "SRem: {}", req),
                BaseOperation::SetBit(req) => write!(f, "SetBit: {}", req),
                BaseOperation::LPop(req) => write!(f, "LPop: {}", req),
            },
            Operation::Redis(op) => match op {
                RedisOperation::RedisSet(req) => write!(f, "RedisSet: {}", req),
                RedisOperation::RedisMset(req) => write!(f, "RedisMset: {}", req),
                RedisOperation::RedisDel(req) => write!(f, "RedisDel: {}", req),
                RedisOperation::RedisRename(req) => write!(f, "RedisRename: {}", req),
                RedisOperation::RedisEval(req) => write!(f, "RedisEval: {}", req),
                RedisOperation::RedisExec(req) => write!(f, "RedisExec: {}", req),
                RedisOperation::RedisBLPop(req) => write!(f, "RedisBLPop: {}", req),
                RedisOperation::RedisRenameNx(req) => write!(f, "RedisRenameNx: {}", req),
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
