use crate::protocol::bf::bf_exits::BfExistsParams;
use crate::protocol::bitmap::bitcount::BitCountParams;
use crate::protocol::bitmap::bitpos::BitPosParams;
use crate::protocol::bitmap::getbit::GetBitParams;
use crate::protocol::hash::hexists::HExistsParams;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hgetall::HGetAllParams;
use crate::protocol::hash::hkeys::HKeysParams;
use crate::protocol::hash::hlen::HLenParams;
use crate::protocol::hash::hmget::HMGetParams;
use crate::protocol::hash::hvals::HValsParams;
use crate::protocol::key::dbsize::DbsizeParams;
use crate::protocol::key::exists::ExistsParams;
use crate::protocol::key::keys::KeysParams;
use crate::protocol::key::pttl::PTtlParams;
use crate::protocol::key::ttl::TtlParams;
use crate::protocol::key::type_::TypeParams;
use crate::protocol::list::lindex::LIndexParams;
use crate::protocol::list::llen::LLenParams;
use crate::protocol::list::lrange::LRangeParams;
use crate::protocol::server::memory::usage::MemoryUsageParams;
use crate::protocol::set::scard::SCardParams;
use crate::protocol::set::sdiff::SDiffParams;
use crate::protocol::set::sinter::SInterParams;
use crate::protocol::set::sismember::SIsMemberParams;
use crate::protocol::set::smembers::SMembersParams;
use crate::protocol::set::srandmember::SRandMemberParams;
use crate::protocol::set::sunion::SUnionParams;
use crate::protocol::string::get::GetParams;
use crate::protocol::string::len::StrLenParams;
use crate::protocol::string::mget::MgetParams;
use crate::protocol::string::pfcount::PfcountParams;
use crate::protocol::zset::zcard::ZCardParams;
use crate::protocol::zset::zcount::ZCountParams;
use crate::protocol::zset::zrange::ZRangeParams;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreParams;
use crate::protocol::zset::zrank::ZRankParams;
use crate::protocol::zset::zrevrank::ZRevRankParams;
use crate::protocol::zset::zscore::ZScoreParams;
use serde::{Deserialize, Serialize};

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
    HGetAll(HGetAllParams),
    HKeys(HKeysParams),
    HVals(HValsParams),
    LLen(LLenParams),
    Type(TypeParams),
    LIndex(LIndexParams),
    SIsMember(SIsMemberParams),
    HExists(HExistsParams),
    PTtl(PTtlParams),
    Ttl(TtlParams),
    HLen(HLenParams),
    BitCount(BitCountParams),
    BitPos(BitPosParams),
    SCard(SCardParams),
    SRandMember(SRandMemberParams),
    SInter(SInterParams),
    SUnion(SUnionParams),
    SDiff(SDiffParams),
    Keys(KeysParams),
    ZScore(ZScoreParams),
    ZCard(ZCardParams),
    ZCount(ZCountParams),
    ZRank(ZRankParams),
    ZRevRank(ZRevRankParams),
    DbSize(DbsizeParams),
    MemoryUsage(MemoryUsageParams),
    PFCount(PfcountParams),
    BfExists(BfExistsParams),
}
