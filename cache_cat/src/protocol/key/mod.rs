pub mod del;

pub mod dbsize;
pub mod exists;
pub mod expire;
pub mod flushall;
pub mod flushdb;
mod insert;
pub mod keys;
pub mod persist;
pub mod pexpire;
pub mod pttl;
pub mod rename;
pub mod renamenx;
pub mod ttl;
pub mod type_;
pub mod unlink;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::EntrySnapshot;
    use crate::raft::types::core::mocha::cas::{ComputeCommand, MultiReadComputeCommand};
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::ValueObject;

    #[test]
    fn expiration_commands_return_integers_in_resp3() {
        let entry = EntrySnapshot {
            value: MyValue::new(ValueObject::String("value".into())),
            expire_at: None,
        };
        let expire = expire::ExpireReq {
            key: "k".into(),
            expires_at: 10,
            condition: None,
        };
        let pexpire = pexpire::PExpireReq {
            key: "k".into(),
            expires_at: 10,
            condition: None,
        };
        let persist = persist::PersistReq { key: "k".into() };
        for reply in [
            expire.clone().mutate(entry.clone(), 0).1,
            pexpire.clone().mutate(entry.clone(), 0).1,
            persist
                .clone()
                .mutate(
                    EntrySnapshot {
                        expire_at: Some(10_000),
                        ..entry.clone()
                    },
                    0,
                )
                .1,
        ] {
            assert_eq!(reply.encode_proto(3), b":1\r\n");
        }
        for reply in [
            expire.init().1,
            pexpire.init().1,
            persist.clone().init().1,
            persist.mutate(entry.clone(), 0).1,
            expire::ExpireReq {
                key: "k".into(),
                expires_at: 10,
                condition: Some(expire::ExpireCondition::Xx),
            }
            .mutate(entry.clone(), 0)
            .1,
            pexpire::PExpireReq {
                key: "k".into(),
                expires_at: 10,
                condition: Some(expire::ExpireCondition::Xx),
            }
            .mutate(entry, 0)
            .1,
        ] {
            assert_eq!(reply.encode_proto(3), b":0\r\n");
        }
    }

    #[test]
    fn rename_missing_source_returns_redis_error_prefix() {
        let (writes, reply) = rename::RenameParams {
            key: "missing".into(),
            new_key: "target".into(),
        }
        .mutate_writes(vec![None], 0);
        assert!(writes.is_empty());
        assert_eq!(reply.encode(), b"-ERR no such key\r\n");
        let (writes, reply) = renamenx::RenameNxParams {
            key: "missing".into(),
            new_key: "target".into(),
        }
        .mutate_writes(vec![None, None], 0);
        assert!(writes.is_empty());
        assert_eq!(reply.encode(), b"-ERR no such key\r\n");
    }
}
