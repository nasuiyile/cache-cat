use crate::protocol::bf::bf_add::BfAddReq;
use crate::protocol::bf::bf_madd::BfMAddReq;
use crate::protocol::bf::bf_reserve::BfReserveReq;
use crate::protocol::bitmap::bitfield::BitFieldReq;
use crate::protocol::bitmap::bitop::BitOpReq;
use crate::protocol::bitmap::setbit::SetBitReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn set_bit(&self, param: SetBitReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn bit_op(&self, param: BitOpReq, update: &mut Update) -> Value {
        self.execute_multi_read_compute(param, update)
    }
    pub fn bit_field(&self, param: BitFieldReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn bf_add(&self, param: BfAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn bf_madd(&self, param: BfMAddReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn bf_reserve(&self, param: BfReserveReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
