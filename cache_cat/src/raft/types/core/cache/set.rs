use crate::protocol::set::sadd::SAddReq;
use crate::protocol::set::srem::SRemReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn s_rem(&self, param: SRemReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn s_add(&self, param: SAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
