use crate::protocol::key::exists::ExistsParams;
use crate::protocol::list::lrange::LRangeParams;
use crate::protocol::string::mget::MgetParams;
use crate::protocol::zset::zrange::ZRangeParams;
use crate::protocol::bitmap::getbit::GetBitParams;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hmget::HMGetParams;
use crate::protocol::set::smembers::SMembersParams;
use crate::protocol::string::get::GetParams;
use serde::{Deserialize, Serialize};
use crate::protocol::string::len::StrLenParams;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreParams;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadOperation {
    Exists(ExistsParams),
    Get(GetParams),
    MGet(MgetParams),
    LRange(LRangeParams),
    ZRange(ZRangeParams),
    HGet(HGetParams),
    SMembers(SMembersParams),
    HMGet(HMGetParams),
    GetBit(GetBitParams),
    ZRangeByScore(ZRangeByScoreParams),
    StrLen(StrLenParams),
}
