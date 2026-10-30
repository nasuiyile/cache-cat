use crate::raft::types::core::mocha::core::{ApplyEffects, MyCache, Update, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::AddId;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};

pub fn read_request(
    my_cache: &MyCache,
    read_operation: ReadOperation,
    db_number: u16,
    read_clock: u64,
) -> Value {
    read_request_inner::<false>(my_cache, read_operation, db_number, read_clock)
}

fn read_request_inner<const REPLICATED: bool>(
    my_cache: &MyCache,
    read_operation: ReadOperation,
    db_number: u16,
    read_clock: u64,
) -> Value {
    match read_operation {
        ReadOperation::Exists(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::Get(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::MGet(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::XRead(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::ZRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SMembers(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HMGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::GetBit(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRangeByScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::StrLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGetAll(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HScan(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HKeys(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HVals(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::LLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Type(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LIndex(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SIsMember(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::PTtl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Ttl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HRandField(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::HStrLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitPos(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SRandMember(param) => {
            my_cache.execute_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::SInter(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::SUnion(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::SDiff(param) => {
            my_cache.execute_multi_read_with_mode::<_, REPLICATED>(param, db_number, read_clock)
        }
        ReadOperation::Keys(param) => {
            if REPLICATED {
                param.execute_with_clock(my_cache, db_number, read_clock)
            } else {
                my_cache.keys(param, db_number, Some(read_clock))
            }
        }
        ReadOperation::ZScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRevRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::DbSize(param) => {
            if REPLICATED {
                param.execute_with_clock(my_cache, db_number, read_clock)
            } else {
                my_cache.dbsize(db_number)
            }
        }
        ReadOperation::MemoryUsage(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::PFCount(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::BfExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfMExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfInfo(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BfScanDump(param) => my_cache.execute_read(param, db_number, read_clock),
    }
}

pub fn base_request(
    my_cache: &MyCache,
    base_operation: BaseOperation,
    update: &mut Update,
) -> Value {
    // Snapshot replay has no outer Lua/EXEC lock to reuse.
    base_request_inner(my_cache, base_operation, update, true)
}

fn base_request_inner(
    my_cache: &MyCache,
    base_operation: BaseOperation,
    update: &mut Update,
    external: bool,
) -> Value {
    match base_operation {
        BaseOperation::Empty => {
            for db in &my_cache.databases {
                db.mocha.active_expire_cycle_blocking();
            }
            Value::ok()
        }
        BaseOperation::Set(param) => my_cache.set(param, update),
        BaseOperation::XAdd(mut param) => {
            // Resolve automatic IDs before both initialization and mutation so
            // every Raft replica uses the replicated write clock.
            if matches!(param.id, AddId::Auto) {
                param.id = AddId::AutoSequence(update.write_clock);
            }
            let key = (update.effects.is_some()
                && !matches!(update.update_type, UpdateType::CAS { .. })
                && update
                    .waiters
                    .as_deref()
                    .is_some_and(|waiters| !waiters.blocking_keys.is_empty()))
            .then(|| (update.db_number, param.key.clone()));
            let result = my_cache.execute_compute(param, update);
            if matches!(&result, Value::BulkString(Some(_)))
                && let Some(key) = key
                && let Some(effects) = update.effects.as_deref_mut()
            {
                // EXEC and Lua must finish before their final stream contents
                // are served to readers; intermediate XADD data can be deleted.
                effects.ready_streams.insert(key);
            }
            result
        }
        BaseOperation::Expire(param) => my_cache.expire(param, update),
        BaseOperation::PExpire(param) => my_cache.p_expire(param, update),
        BaseOperation::LPush(param) => my_cache.l_push(param, update),
        BaseOperation::Del(param) => my_cache.del(param, update),
        BaseOperation::Incr(param) => my_cache.incr(param, update),
        BaseOperation::IncrBy(param) => my_cache.incr_by(param, update),
        BaseOperation::IncrByFloat(param) => my_cache.execute_compute(param, update),
        BaseOperation::Append(param) => my_cache.append(param, update),
        BaseOperation::HSet(param) => my_cache.h_set(param, update),
        BaseOperation::HIncr(param) => my_cache.h_incr(param, update),
        BaseOperation::HIncrByFloat(param) => my_cache.execute_compute(param, update),
        BaseOperation::ZAdd(param) => my_cache.z_add(param, update),
        BaseOperation::SAdd(param) => my_cache.s_add(param, update),
        BaseOperation::Persist(param) => my_cache.persist(param, update),
        BaseOperation::Insert(param) => my_cache.insert(param, update),
        BaseOperation::HDel(param) => my_cache.h_del(param, update),
        BaseOperation::SRem(param) => my_cache.s_rem(param, update),
        BaseOperation::SetBit(param) => my_cache.set_bit(param, update),
        BaseOperation::LPop(param) => my_cache.l_pop(param, update),
        BaseOperation::RPush(param) => my_cache.r_push(param, update),
        BaseOperation::RPop(param) => my_cache.r_pop(param, update),
        BaseOperation::LRem(param) => my_cache.l_rem(param, update),
        BaseOperation::LSet(param) => my_cache.l_set(param, update),
        BaseOperation::DecrBy(param) => my_cache.decr_by(param, update),
        BaseOperation::HSetNx(param) => my_cache.h_set_nx(param, update),
        BaseOperation::Decr(param) => my_cache.decr(param, update),
        BaseOperation::ZRem(param) => my_cache.z_rem(param, update),
        BaseOperation::LTrim(param) => my_cache.l_trim(param, update),
        BaseOperation::FlushDB(param) => my_cache.flush_db(param, update, external),
        BaseOperation::FlushAll(param) => my_cache.flush_all(param, update, external),
        BaseOperation::BitField(param) => my_cache.bit_field(param, update),
        BaseOperation::SPop(param) => my_cache.s_pop(param, update),
        BaseOperation::ZPopMin(param) => my_cache.z_pop_min(param, update),
        BaseOperation::Unlink(param) => my_cache.unlink(param, update),
        BaseOperation::ZIncrBy(param) => my_cache.z_incr_by(param, update),
        BaseOperation::HMSet(param) => my_cache.h_mset(param, update),
        BaseOperation::PfAdd(param) => my_cache.p_f_add(param, update),
        BaseOperation::BfAdd(param) => my_cache.bf_add(param, update),
        BaseOperation::BfMAdd(param) => my_cache.bf_madd(param, update),
        BaseOperation::BfReserve(param) => my_cache.bf_reserve(param, update),
        BaseOperation::BfInsert(param) => my_cache.bf_insert(param, update),
        BaseOperation::BfLoadChunk(param) => my_cache.bf_load(param, update),
        BaseOperation::XGroup(param) => my_cache.x_group(param, update),
    }
}

#[inline]
pub fn do_request(
    my_cache: &MyCache,
    operation: Operation,
    update: &mut Update,
    external: bool, //用来防止多次加锁
) -> Value {
    let result = match operation {
        // Reads inside EXEC/Lua share the log's clock and deterministic order/RNG.
        Operation::Read(read) => {
            read_request_inner::<true>(my_cache, read, update.db_number, update.write_clock)
        }
        Operation::Base(base) => base_request_inner(my_cache, base, update, external),
        Operation::Redis(redis) => match redis {
            RedisOperation::RedisDel(param) => my_cache.redis_del(param, update, external),
            RedisOperation::RedisSet(param) => my_cache.redis_set(param, update),
            RedisOperation::RedisSetNx(param) => my_cache.redis_setnx(param, update),
            RedisOperation::RedisSetEx(param) => my_cache.redis_setex(param, update),
            RedisOperation::RedisPSetEx(param) => my_cache.redis_psetex(param, update),
            RedisOperation::RedisGetSet(param) => my_cache.redis_get_set(param, update),
            RedisOperation::RedisMset(param) => my_cache.redis_mset(param, update, external),
            RedisOperation::RedisBitOp(param) => my_cache.execute_multi_read_compute(param, update),
            RedisOperation::RedisSInterStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisSUnionStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisSDiffStore(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisPFMerge(param) => {
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisRename(param) => {
                let _lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisRenameNx(param) => {
                let _lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache.execute_multi_read_compute(param, update)
            }
            RedisOperation::RedisEval(param) => {
                let _exclusive_lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                my_cache
                    .lua_env
                    .exec_lua(
                        my_cache,
                        &param.script,
                        &param.keys,
                        &param.args,
                        update,
                        param.proto,
                    )
                    .unwrap_or_else(|err| err.into())
            }
            RedisOperation::RedisExec(param) => {
                let _exclusive_lock = if external {
                    Some(my_cache.read_lock.write())
                } else {
                    None
                };
                let mut vec = Vec::new();
                for queued in param.operations {
                    update.db_number = queued.db_number;
                    vec.push(do_request(my_cache, queued.operation, update, false));
                }
                Value::Array(Some(vec))
            }
            RedisOperation::RedisUnlink(param) => my_cache.redis_unlink(param, update, external),
            RedisOperation::RedisReply(value) => value,
            RedisOperation::RedisInfo(param) => param.render_from_cache(my_cache),
            RedisOperation::RedisEvalSha { .. } => {
                // Unresolved hashes must never consult replica-local script caches.
                Value::error("unresolved EVALSHA in replicated request")
            }
        },
    };
    if external && let Some(effects) = update.effects.as_deref_mut() {
        // The state machine owns this context under its existing write_lock.
        // Empty sets require no registry access, atomics, allocation or locks;
        // drain retains capacity for later commands in the same apply batch.
        if !effects.ready_streams.is_empty() {
            // Capture owned responses before a following DEL can erase data.
            let waiters = update
                .waiters
                .as_deref_mut()
                .expect("effects require an execution guard");
            for key in effects.ready_streams.drain() {
                waiters.blocking_keys.wake_ready(&key, |params| {
                    let result =
                        my_cache.execute_multi_read(params.clone(), key.0, update.write_clock);
                    (!matches!(result, Value::Array(None))).then_some(result)
                });
            }
            ApplyEffects::recycle_ready_keys(&mut effects.ready_streams);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::{EntrySnapshot, ExpirePolicy};
    use crate::protocol::key::del::DelReq;
    use crate::protocol::key::persist::PersistReq;
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::stream::xadd::XAddReq;
    use crate::protocol::stream::xread::{XReadId, XReadParams};
    use crate::protocol::transaction::QueuedOperation;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::core::{ApplyEffects, MyValue, UpdateType};
    use crate::raft::types::core::structure::stream::StreamId;
    use crate::raft::types::core::value_object::ValueObject;
    use bytes::Bytes;
    use std::time::Duration;

    fn xadd(key: &Bytes, id: AddId) -> Operation {
        Operation::Base(BaseOperation::XAdd(XAddReq {
            key: key.clone(),
            id,
            fields: vec![(b"field".to_vec(), b"value".to_vec())],
        }))
    }

    async fn apply(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
    ) -> Value {
        apply_with_effects(
            cache,
            operation,
            db_number,
            write_clock,
            &mut ApplyEffects::default(),
        )
        .await
    }

    async fn apply_with_effects(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
        effects: &mut ApplyEffects,
    ) -> Value {
        apply_inner_with_effects(cache, operation, db_number, write_clock, effects, true).await
    }

    async fn apply_inner_with_effects(
        cache: &MyCache,
        operation: Operation,
        db_number: u16,
        write_clock: u64,
        effects: &mut ApplyEffects,
        external: bool,
    ) -> Value {
        let mut write_guard = cache.write_lock.lock().await;
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number,
            write_clock,
            update_type: &mut update_type,
            effects: Some(effects),
            waiters: Some(&mut write_guard),
        };
        do_request(cache, operation, &mut update, external)
    }

    #[test]
    fn snapshot_restore_replays_persist_before_expiration() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"restore-key");

        // Make the existing time wheel use a clock newer than the snapshot.
        cache.set_write_clock(1_000);
        cache.databases[0].mocha.active_expire_cycle_blocking();
        cache.pause_expire_workers();
        cache.reset_write_clock();

        cache.databases[0].mocha.insert_snapshot(
            key.clone(),
            EntrySnapshot {
                value: MyValue::new(ValueObject::String(Bytes::from_static(b"v"))),
                expire_at: Some(500),
            },
        );

        // Force the worker to process its queue at a clock beyond the deadline:
        // even then, a paused worker must not delete the key before replay.
        cache.set_write_clock(1_000);
        cache.databases[0].mocha.active_expire_cycle_blocking();
        cache.reset_write_clock();
        cache.set_write_clock(400);

        let mut update_type = UpdateType::CAS {
            expected_revision: crate::utils::OptionalU64::some(0),
            revision: 1,
        };
        let mut update = Update {
            db_number: 0,
            write_clock: 400,
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let reply = base_request(
            &cache,
            BaseOperation::Persist(PersistReq { key: key.clone() }),
            &mut update,
        );
        assert!(matches!(reply, Value::Integer(1)));

        cache.set_write_clock(1_000);
        cache.resume_expire_workers();
        let entry = cache.databases[0]
            .mocha
            .get_entry(&key)
            .expect("persisted snapshot key");
        assert_eq!(entry.expire_at, None);
    }

    fn read_params(key: &Bytes) -> XReadParams {
        XReadParams {
            keys: vec![key.clone()],
            ids: vec![XReadId::After(StreamId::ZERO)],
            count: Some(1000),
            max_count: None,
            max_size: None,
            block_ms: Some(0),
            resp_version: 2,
        }
    }

    fn expected_reply(id: &str) -> Vec<u8> {
        format!(
            "*1\r\n*2\r\n$6\r\nstream\r\n*1\r\n*2\r\n${}\r\n{id}\r\n*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n",
            id.len()
        ).into_bytes()
    }

    #[tokio::test]
    async fn xadd_notifies_all_readers_only_in_its_database() {
        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        let first = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        let second = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        let mut other_db = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(1, key.clone())], None, read_params(&key))
            .unwrap();

        let result = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        assert_eq!(result.encode(), b"$6\r\n1234-0\r\n");
        for registration in [first, second] {
            let result = tokio::time::timeout(Duration::from_secs(1), registration.wait())
                .await
                .expect("successful XADD must notify every reader");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert_eq!(reply.encode(), expected_reply("1234-0"));
        }
        assert!(
            other_db.cancel().await,
            "another database must remain blocked"
        );
        let entry = cache.databases[0].mocha.get_entry(&key).unwrap();
        let ValueObject::Stream(stream) = entry.value.data else {
            panic!("the stream must be stored before notifying readers");
        };
        assert_eq!(stream.read().last_generated_id(), StreamId::new(1234, 0));
    }

    #[tokio::test]
    async fn failed_xadd_keeps_readers_registered() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let id = AddId::Explicit(StreamId::new(10, 0));
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10).await,
            Value::BulkString(Some(_))
        ));

        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10).await,
            Value::Error(_)
        ));
        assert!(
            reader.cancel().await,
            "a rejected stream ID must not wake readers"
        );

        cache.databases[0].mocha.insert_entry(
            key.clone(),
            MyValue::new(ValueObject::String(Bytes::from_static(b"wrong type"))),
            ExpirePolicy::Persistent,
        );
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, AddId::Auto), 0, 11).await,
            Value::Error(_)
        ));
        assert!(reader.cancel().await, "a type error must not wake readers");
    }

    #[tokio::test]
    async fn xadd_in_exec_and_lua_notifies_readers() {
        let cache = MyCache::new(2).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            (
                Operation::Redis(RedisOperation::RedisExec(ExecParams {
                    operations: vec![QueuedOperation::new(0, xadd(&key, AddId::Auto))],
                })),
                1,
            ),
            (
                Operation::Redis(RedisOperation::RedisEval(EvalParams {
                    script: "return redis.call('XADD', KEYS[1], '*', 'field', 'value')".into(),
                    numkeys: 1,
                    keys: vec![key.clone()],
                    args: vec![],
                    proto: 2,
                })),
                0,
            ),
        ];
        for (operation, db_number) in operations {
            let mut params = read_params(&key);
            if let Some(entry) = cache.databases[0].mocha.get_entry(&key) {
                let ValueObject::Stream(stream) = entry.value.data else {
                    unreachable!()
                };
                params.ids[0] = XReadId::After(stream.read().last_generated_id());
            }
            let reader = cache
                .write_lock
                .lock()
                .await
                .register_stream(vec![(0, key.clone())], None, params)
                .unwrap();
            let result = apply(&cache, operation, db_number, 1234).await;
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            let result = tokio::time::timeout(Duration::from_secs(1), reader.wait())
                .await
                .expect("nested XADD must notify readers");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert!(matches!(reply, Value::MapWithResp2 { .. }));
            assert!(cache.databases[1].mocha.get_entry(&key).is_none());
        }
    }

    #[tokio::test]
    async fn completed_xadd_reply_survives_a_following_del() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        let deleted = apply(
            &cache,
            Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
            0,
            1235,
        )
        .await;
        assert!(matches!(deleted, Value::Integer(1)));
        assert!(cache.databases[0].mocha.get_entry(&key).is_none());

        let (_, reply) = tokio::time::timeout(Duration::from_secs(1), reader.wait())
            .await
            .expect("the reply was produced before DEL")
            .unwrap();
        assert_eq!(reply.encode(), expected_reply("1234-0"));
    }

    #[tokio::test]
    async fn xadd_keeps_a_reader_until_its_cursor_has_new_entries() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let mut params = read_params(&key);
        params.ids[0] = XReadId::After(StreamId::new(2000, 0));
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, key.clone())], None, params)
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
        apply(&cache, xadd(&key, AddId::Auto), 0, 2001).await;

        let (_, reply) = tokio::time::timeout(Duration::from_secs(1), reader.wait())
            .await
            .expect("the later XADD must satisfy the same registration")
            .unwrap();
        assert_eq!(reply.encode(), expected_reply("2001-0"));
    }

    #[tokio::test]
    async fn exec_and_lua_do_not_publish_intermediate_xadd_data() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    QueuedOperation::new(0, xadd(&key, AddId::Auto)),
                    QueuedOperation::new(
                        0,
                        Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
                    ),
                ],
            })),
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script: "redis.call('XADD', KEYS[1], '*', 'field', 'value'); return redis.call('DEL', KEYS[1])".into(),
                numkeys: 1,
                keys: vec![key.clone()],
                args: vec![],
                proto: 2,
            })),
        ];
        for operation in operations {
            let mut reader = cache
                .write_lock
                .lock()
                .await
                .register_stream(vec![(0, key.clone())], None, read_params(&key))
                .unwrap();
            let result = apply(&cache, operation, 0, 1234).await;
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            assert!(cache.databases[0].mocha.get_entry(&key).is_none());
            assert!(
                reader.cancel().await,
                "an intermediate XADD must not fulfill the read"
            );
        }
    }

    #[tokio::test]
    async fn automatic_xadd_ids_use_the_replicated_clock_for_new_and_existing_streams() {
        let key = Bytes::from_static(b"stream");
        for _ in 0..2 {
            let cache = MyCache::new(1).unwrap();
            let initial = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
            let next = apply(&cache, xadd(&key, AddId::Auto), 0, 1234).await;
            assert_eq!(initial.encode(), b"$6\r\n1234-0\r\n");
            assert_eq!(next.encode(), b"$6\r\n1234-1\r\n");
        }
    }

    #[tokio::test]
    async fn one_exec_publishes_stream_notifications_in_each_database() {
        use futures::FutureExt;
        let cache = MyCache::new(2).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut readers = Vec::new();
        for db in 0..2 {
            readers.push(
                cache
                    .write_lock
                    .lock()
                    .await
                    .register_stream(vec![(db, stream.clone())], None, read_params(&stream))
                    .unwrap(),
            );
        }
        let mut effects = ApplyEffects::default();
        let result = apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    QueuedOperation::new(0, xadd(&stream, AddId::Explicit(StreamId::new(10, 0)))),
                    QueuedOperation::new(1, xadd(&stream, AddId::Explicit(StreamId::new(20, 0)))),
                ],
            })),
            0,
            100,
            &mut effects,
        )
        .await;
        assert!(matches!(result, Value::Array(Some(_))));
        assert!(effects.ready_streams.is_empty());
        for (db, reader) in readers.into_iter().enumerate() {
            let (ready_key, reply) = reader.wait().now_or_never().unwrap().unwrap();
            assert_eq!(ready_key, (db as u16, stream.clone()));
            assert_eq!(
                reply.encode(),
                expected_reply(if db == 0 { "10-0" } else { "20-0" })
            );
        }
    }

    #[tokio::test]
    async fn lua_error_still_publishes_completed_xadd_and_drains_effects() {
        use futures::FutureExt;
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, stream.clone())], None, read_params(&stream))
            .unwrap();
        let mut effects = ApplyEffects::default();
        let result = apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script:
                    "redis.call('XADD', KEYS[1], '*', 'field', 'value'); error('stop after write')"
                        .into(),
                numkeys: 1,
                keys: vec![stream],
                args: vec![],
                proto: 2,
            })),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(matches!(result, Value::Error(_)));
        let (_, response) = reader.wait().now_or_never().unwrap().unwrap();
        assert_eq!(response.encode(), expected_reply("1234-0"));
        assert!(effects.ready_streams.is_empty());
    }

    #[tokio::test]
    async fn reused_effects_keep_capacity_without_republishing_previous_xadd() {
        use crate::protocol::string::set::SetReq;
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, stream.clone())], None, read_params(&stream))
            .unwrap();
        let mut effects = ApplyEffects::default();
        apply_inner_with_effects(
            &cache,
            xadd(&stream, AddId::Auto),
            0,
            1234,
            &mut effects,
            false,
        )
        .await;
        let capacity = effects.ready_streams.capacity();
        assert!(capacity > 0);
        assert!(reader.cancel().await);
        apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisReply(Value::ok())),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(effects.ready_streams.is_empty());
        assert_eq!(effects.ready_streams.capacity(), capacity);

        // Register directly so stale effects would produce an observable
        // response from the stream; an unrelated SET must not serve it.
        let mut later_reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, stream.clone())], None, read_params(&stream))
            .unwrap();
        apply_with_effects(
            &cache,
            Operation::Base(BaseOperation::Set(SetReq {
                key: Bytes::from_static(b"unrelated"),
                value: Bytes::from_static(b"v"),
                ex_time: 0,
            })),
            0,
            1234,
            &mut effects,
        )
        .await;
        assert!(later_reader.cancel().await);
        assert_eq!(effects.ready_streams.capacity(), capacity);
    }

    #[tokio::test]
    async fn replayed_xadd_does_not_leak_notifications_to_live_apply() {
        let cache = MyCache::new(1).unwrap();
        let stream = Bytes::from_static(b"stream");
        let mut reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, stream.clone())], None, read_params(&stream))
            .unwrap();
        let mut update_type = UpdateType::CAS {
            expected_revision: crate::utils::OptionalU64::NONE,
            revision: 1,
        };
        let mut update = Update {
            db_number: 0,
            write_clock: 1234,
            update_type: &mut update_type,
            effects: None,
            waiters: None,
        };
        let Operation::Base(stream_write) = xadd(&stream, AddId::Explicit(StreamId::new(1234, 0)))
        else {
            unreachable!()
        };
        assert!(matches!(
            base_request(&cache, stream_write, &mut update),
            Value::BulkString(Some(_))
        ));
        let mut effects = ApplyEffects::default();
        apply_with_effects(
            &cache,
            Operation::Redis(RedisOperation::RedisReply(Value::ok())),
            0,
            1235,
            &mut effects,
        )
        .await;
        assert!(reader.cancel().await);
        assert!(effects.ready_streams.is_empty());
        assert_eq!(effects.ready_streams.capacity(), 0);
    }

    #[tokio::test]
    async fn large_stream_notification_batch_releases_capacity_before_small_commands() {
        use futures::FutureExt;
        let cache = MyCache::new(1).unwrap();
        let keys: Vec<_> = (0..128)
            .map(|i| Bytes::from(format!("stream-{i}")))
            .collect();
        let mut params = read_params(&keys[0]);
        params.keys = keys.clone();
        params.ids = vec![XReadId::After(StreamId::ZERO); keys.len()];
        let reader = cache
            .write_lock
            .lock()
            .await
            .register_stream(
                keys.iter().map(|key| (0, key.clone())).collect(),
                None,
                params,
            )
            .unwrap();
        let mut effects = ApplyEffects::default();
        let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: keys
                .iter()
                .map(|key| QueuedOperation::new(0, xadd(key, AddId::Auto)))
                .collect(),
        }));
        assert!(matches!(
            apply_with_effects(&cache, operation, 0, 1, &mut effects).await,
            Value::Array(Some(_))
        ));
        let (_, reply) = reader.wait().now_or_never().unwrap().unwrap();
        assert!(
            matches!(reply, Value::MapWithResp2 { entries, .. } if entries.len() == keys.len())
        );
        assert!(effects.ready_streams.is_empty());
        assert_eq!(effects.ready_streams.capacity(), 0);

        let mut params = read_params(&keys[0]);
        params.ids[0] = XReadId::After(StreamId::new(1, 0));
        let next = cache
            .write_lock
            .lock()
            .await
            .register_stream(vec![(0, keys[0].clone())], None, params)
            .unwrap();
        apply_with_effects(&cache, xadd(&keys[0], AddId::Auto), 0, 2, &mut effects).await;
        assert!(next.wait().now_or_never().unwrap().is_ok());
        assert!(effects.ready_streams.capacity() > 0);
        assert!(effects.ready_streams.capacity() <= 64);
    }
}
