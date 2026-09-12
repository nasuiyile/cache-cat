use crate::protocol::stream::xgroup::XGroupReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn x_group(&self, param: XGroupReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
