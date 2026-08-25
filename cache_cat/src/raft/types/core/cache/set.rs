use crate::protocol::set::sadd::SAddReq;
use crate::protocol::set::sdiffstore::SDiffStoreReq;
use crate::protocol::set::sinterstore::SInterStoreReq;
use crate::protocol::set::srem::SRemReq;
use crate::protocol::set::sunionstore::SUnionStoreReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn s_rem(&self, param: SRemReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn s_add(&self, param: SAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn s_inter_store(&self, param: SInterStoreReq, update: &mut Update) -> Value {
        self.execute_multi_read_compute(param, update)
    }

    pub fn s_union_store(&self, param: SUnionStoreReq, update: &mut Update) -> Value {
        self.execute_multi_read_compute(param, update)
    }

    pub fn s_diff_store(&self, param: SDiffStoreReq, update: &mut Update) -> Value {
        self.execute_multi_read_compute(param, update)
    }
}
