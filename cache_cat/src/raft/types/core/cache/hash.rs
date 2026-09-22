use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hmset::HMSetReq;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::hash::hsetnx::HSetNxReq;
use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;

impl MyCache {
    pub fn h_del(&self, param: HDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_set(&self, param: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_set_nx(&self, param: HSetNxReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_incr(&self, param: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_mset(&self, param: HMSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::hash::hget::HGetParams;
    use crate::protocol::key::del::DelReq;
    use crate::raft::types::core::mocha::core::UpdateType;
    use crate::raft::types::core::mocha::request_handler::do_request;
    use crate::raft::types::entry::base_operation::BaseOperation;
    use crate::raft::types::entry::read_operation::ReadOperation;
    use crate::raft::types::entry::request::Operation;
    use bytes::Bytes;

    fn apply(cache: &MyCache, operation: Operation) -> Vec<u8> {
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: 1,
            update_type: &mut update_type,
        };
        do_request(cache, operation, &mut update, true).encode()
    }

    fn hset(cache: &MyCache, key: &'static str, field: &'static str, value: &str) -> Vec<u8> {
        let req = HSetReq {
            key: key.into(),
            elements: vec![(field.into(), Bytes::copy_from_slice(value.as_bytes()))],
        };
        apply(cache, Operation::Base(BaseOperation::HSet(req)))
    }

    fn hsetnx(cache: &MyCache, key: &'static str, field: &'static str, value: &str) -> Vec<u8> {
        let req = HSetNxReq {
            key: key.into(),
            field: field.into(),
            value: Bytes::copy_from_slice(value.as_bytes()),
        };
        apply(cache, Operation::Base(BaseOperation::HSetNx(req)))
    }

    fn hget(cache: &MyCache, key: &'static str, field: &'static str) -> Vec<u8> {
        let params = HGetParams {
            key: key.into(),
            field: field.into(),
        };
        apply(cache, Operation::Read(ReadOperation::HGet(params)))
    }

    fn bulk(value: &str) -> Vec<u8> {
        format!("${}\r\n{}\r\n", value.len(), value).into_bytes()
    }

    #[test]
    fn hset_and_hsetnx_keep_field_values_verbatim() {
        let cache = MyCache::new(1).expect("cache");

        // Only canonical integers may use the integer encoding; everything else
        // must read back byte-for-byte (`HSET h f "01"` is not `"1"`).
        for value in [
            "01",
            "+1",
            "-0",
            " 1",
            "1 ",
            "007",
            "0",
            "-1",
            "9223372036854775807",
        ] {
            // Both the key-creating path (`init`) and the existing-hash path (`mutate`).
            assert_eq!(hset(&cache, "h", "f", value), b":1\r\n");
            assert_eq!(hget(&cache, "h", "f"), bulk(value), "HSET init {value:?}");
            assert_eq!(hset(&cache, "h", "g", value), b":1\r\n");
            assert_eq!(hget(&cache, "h", "g"), bulk(value), "HSET mutate {value:?}");

            assert_eq!(hsetnx(&cache, "nx", "f", value), b":1\r\n");
            assert_eq!(
                hget(&cache, "nx", "f"),
                bulk(value),
                "HSETNX init {value:?}"
            );
            assert_eq!(hsetnx(&cache, "nx", "g", value), b":1\r\n");
            assert_eq!(
                hget(&cache, "nx", "g"),
                bulk(value),
                "HSETNX mutate {value:?}"
            );
            assert_eq!(hsetnx(&cache, "nx", "g", "other"), b":0\r\n");
            assert_eq!(hget(&cache, "nx", "g"), bulk(value));

            for key in ["h", "nx"] {
                let del = DelReq { key: key.into() };
                apply(&cache, Operation::Base(BaseOperation::Del(del)));
            }
        }
    }
}
