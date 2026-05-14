use crate::protocol::key::exists::ExistsParams;
use crate::protocol::list::lrange::LRangeParams;
use crate::protocol::string::get::GetParams;
use crate::protocol::string::mget::MgetParams;
use crate::protocol::zset::zrange::ZRangeParams;

use crate::protocol::hash::hget::HGetParams;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadOperation {
    Exists(ExistsParams),
    Get(GetParams),
    MGet(MgetParams),
    LRange(LRangeParams),
    ZRange(ZRangeParams),
    HGet(HGetParams),
}
