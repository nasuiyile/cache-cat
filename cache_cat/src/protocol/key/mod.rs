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
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::raft_command::RaftCommandFactory;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::cas::{ComputeCommand, MultiReadComputeCommand};
    use crate::raft::types::core::mocha::core::{MyCache, MyValue, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::{base_request, do_request};
    use crate::raft::types::core::response_value::Value;
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::base_operation::BaseOperation;
    use crate::raft::types::entry::request::{Operation, RedisOperation};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn completes_without_deadlock(check: impl FnOnce() + Send + 'static) {
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            check();
            let _ = tx.send(());
        });
        // The request is synchronous: an async timeout cannot interrupt a
        // thread that is blocked while reacquiring the parking_lot write lock.
        rx.recv_timeout(Duration::from_secs(5))
            .expect("FLUSH must complete without reacquiring an outer write lock");
        worker.join().unwrap();
    }

    fn command(parts: &[&str]) -> Operation {
        let args: Vec<_> = parts
            .iter()
            .map(|part| Value::BulkString(Some(part.as_bytes().to_vec().into())))
            .collect();
        RaftCommandFactory::init_lua().parse_request(&args).unwrap()
    }

    fn flush(all: bool) -> BaseOperation {
        if all {
            BaseOperation::FlushAll(flushall::FlushAllReq { async_mode: false })
        } else {
            BaseOperation::FlushDB(flushdb::FlushDBReq { async_mode: false })
        }
    }

    fn seeded_cache() -> MyCache {
        let cache = MyCache::new(2).unwrap();
        for database in &cache.databases {
            database.mocha.insert_persistent(
                "old".into(),
                MyValue::new(ValueObject::String("old value".into())),
            );
        }
        cache
    }

    fn assert_flush_scope(cache: &MyCache, all: bool) {
        assert!(cache.databases[1].mocha.get_entry(&b"old"[..]).is_none());
        assert_eq!(
            cache.databases[0].mocha.get_entry(&b"old"[..]).is_none(),
            all,
            "FLUSHDB must preserve the other database; FLUSHALL must clear it"
        );
        assert!(
            cache.read_lock.try_write().is_some(),
            "lock must be released"
        );
    }

    #[test]
    fn flush_in_lua_and_exec_reuses_the_outer_write_lock() {
        completes_without_deadlock(|| {
            let mut cases = Vec::new();
            for call in ["call", "pcall"] {
                let eval = Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
                    format!(
                        "local flushed = redis.{call}('FLUSHDB'); \
                         redis.call('SET', 'after', 'new'); \
                         return {{flushed, redis.call('GET', 'after')}}"
                    )
                    .into(),
                    0,
                    vec![],
                    vec![],
                )));
                cases.push((eval.clone(), false, "*2\r\n+OK\r\n$3\r\nnew\r\n"));
                cases.push((
                    Operation::Redis(RedisOperation::RedisExec(ExecParams {
                        operations: vec![eval],
                    })),
                    false,
                    "*1\r\n*2\r\n+OK\r\n$3\r\nnew\r\n",
                ));
            }
            for all in [false, true] {
                cases.push((
                    Operation::Redis(RedisOperation::RedisExec(ExecParams {
                        operations: vec![
                            Operation::Base(flush(all)),
                            command(&["SET", "after", "new"]),
                            command(&["GET", "after"]),
                        ],
                    })),
                    all,
                    "*3\r\n+OK\r\n+OK\r\n$3\r\nnew\r\n",
                ));
            }
            for (operation, all, expected) in cases {
                let cache = seeded_cache();
                let mut update_type = UpdateType::None;
                let mut update = Update {
                    db_number: 1,
                    write_clock: 1,
                    update_type: &mut update_type,
                };
                let reply = do_request(&cache, operation, &mut update, true);
                assert_eq!(reply.encode(), expected.as_bytes());
                assert_flush_scope(&cache, all);
                let reply = do_request(&cache, command(&["GET", "after"]), &mut update, true);
                assert_eq!(reply.encode(), b"$3\r\nnew\r\n");
            }
        });
    }

    #[test]
    fn flush_standalone_and_snapshot_replay_preserve_database_scope() {
        completes_without_deadlock(|| {
            for all in [false, true] {
                for replay in [false, true] {
                    let cache = seeded_cache();
                    let mut update_type = if replay {
                        UpdateType::CAS(1)
                    } else {
                        UpdateType::None
                    };
                    let mut update = Update {
                        db_number: 1,
                        write_clock: 1,
                        update_type: &mut update_type,
                    };
                    let reply = if replay {
                        base_request(&cache, flush(all), &mut update)
                    } else {
                        do_request(&cache, Operation::Base(flush(all)), &mut update, true)
                    };
                    assert_eq!(reply.encode(), b"+OK\r\n");
                    assert_flush_scope(&cache, all);
                    *update.update_type = UpdateType::None;
                    let reply =
                        do_request(&cache, command(&["SET", "after", "new"]), &mut update, true);
                    assert_eq!(reply.encode(), b"+OK\r\n");
                    let reply = do_request(&cache, command(&["GET", "after"]), &mut update, true);
                    assert_eq!(reply.encode(), b"$3\r\nnew\r\n");
                }
            }
        });
    }

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

    #[test]
    fn expire_overflow_is_rejected_before_missing_key_lookup() {
        let cache = MyCache::new(1).expect("cache");
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number: 0,
            write_clock: i64::MAX as u64,
            update_type: &mut update_type,
        };
        let reply = cache.expire(
            expire::ExpireReq {
                key: "missing".into(),
                expires_at: 1,
                condition: None,
            },
            &mut update,
        );
        assert_eq!(
            reply.encode(),
            b"-ERR invalid expire time in 'expire' command\r\n"
        );
        assert!(
            cache.databases[0]
                .mocha
                .get_entry(&b"missing"[..])
                .is_none()
        );
    }
}
