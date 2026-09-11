use crate::raft::types::core::mocha::core::{MyCache, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::stream::AddId;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};

pub fn read_request(
    my_cache: &MyCache,
    read_operation: ReadOperation,
    db_number: u16,
    read_clock: Option<u64>,
) -> Value {
    match read_operation {
        ReadOperation::Exists(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::Get(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::MGet(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::XRead(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::ZRange(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SMembers(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HMGet(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::GetBit(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRangeByScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::StrLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HGetAll(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HKeys(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HVals(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Type(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::LIndex(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SIsMember(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HExists(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::PTtl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::Ttl(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::HLen(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::BitPos(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SRandMember(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::SInter(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::SUnion(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::SDiff(param) => my_cache.execute_multi_read(param, db_number, read_clock),
        ReadOperation::Keys(param) => my_cache.keys(param, db_number, read_clock),
        ReadOperation::ZScore(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZCard(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZCount(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::ZRevRank(param) => my_cache.execute_read(param, db_number, read_clock),
        ReadOperation::DbSize(_param) => my_cache.dbsize(db_number),
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
            let key = param.key.clone();
            let result = my_cache.execute_compute(param, update);
            if matches!(&result, Value::BulkString(Some(_))) {
                // EXEC and Lua must finish before their final stream contents
                // are served to readers; intermediate XADD data can be deleted.
                my_cache
                    .ready_streams
                    .lock()
                    .insert((update.db_number, key));
            }
            result
        }
        BaseOperation::Expire(param) => my_cache.expire(param, update),
        BaseOperation::PExpire(param) => my_cache.p_expire(param, update),
        BaseOperation::LPush(param) => my_cache.l_push(param, update),
        BaseOperation::Del(param) => my_cache.del(param, update),
        BaseOperation::Incr(param) => my_cache.incr(param, update),
        BaseOperation::IncrBy(param) => my_cache.incr_by(param, update),
        BaseOperation::Append(param) => my_cache.append(param, update),
        BaseOperation::HSet(param) => my_cache.h_set(param, update),
        BaseOperation::HIncr(param) => my_cache.h_incr(param, update),
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
        BaseOperation::FlushDB(param) => my_cache.flush_db(param, update),
        BaseOperation::FlushAll(param) => my_cache.flush_all(param, update),
        BaseOperation::BitField(param) => my_cache.bit_field(param, update),
        BaseOperation::SPop(param) => my_cache.s_pop(param, update),
        BaseOperation::ZPopMin(param) => my_cache.z_pop_min(param, update),
        BaseOperation::Unlink(param) => my_cache.unlink(param, update),
        BaseOperation::ZIncrBy(param) => my_cache.z_incr_by(param, update),
        BaseOperation::HMSet(param) => my_cache.h_mset(param, update),
        BaseOperation::PfAdd(param) => my_cache.p_f_add(param, update),
        BaseOperation::SInterStore(param) => my_cache.s_inter_store(param, update),
        BaseOperation::SUnionStore(param) => my_cache.s_union_store(param, update),
        BaseOperation::SDiffStore(param) => my_cache.s_diff_store(param, update),
        BaseOperation::PFMerge(param) => my_cache.p_f_merge(param, update),
        BaseOperation::BitOp(param) => my_cache.bit_op(param, update),
        BaseOperation::BfAdd(param) => my_cache.bf_add(param, update),
        BaseOperation::BfMAdd(param) => my_cache.bf_madd(param, update),
        BaseOperation::BfReserve(param) => my_cache.bf_reserve(param, update),
        BaseOperation::BfInsert(param) => my_cache.bf_insert(param, update),
        BaseOperation::BfLoadChunk(param) => my_cache.bf_load(param, update),
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
        Operation::Read(read) => read_request(my_cache, read, update.db_number, None),
        Operation::Base(base) => base_request(my_cache, base, update),
        Operation::Redis(redis) => match redis {
            RedisOperation::RedisDel(param) => my_cache.redis_del(param, update, external),
            RedisOperation::RedisSet(param) => my_cache.redis_set(param, update),
            RedisOperation::RedisSetNx(param) => my_cache.redis_setnx(param, update),
            RedisOperation::RedisSetEx(param) => my_cache.redis_setex(param, update),
            RedisOperation::RedisPSetEx(param) => my_cache.redis_psetex(param, update),
            RedisOperation::RedisGetSet(param) => my_cache.redis_get_set(param, update),
            RedisOperation::RedisMset(param) => my_cache.redis_mset(param, update, external),
            RedisOperation::RedisRename(param) => my_cache.redis_rename(param, update, external),
            RedisOperation::RedisRenameNx(param) => {
                my_cache.redis_rename_nx(param, update, external)
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
                for operation in param.operations {
                    vec.push(do_request(my_cache, operation, update, false));
                }
                Value::Array(Some(vec))
            }
            RedisOperation::RedisUnlink(param) => my_cache.redis_unlink(param, update, external),
        },
    };
    if external {
        // The state machine holds write_lock for the whole outer command.
        // Build owned replies here so a following DEL cannot erase the data
        // before a blocked connection has a chance to run.
        let ready = std::mem::take(&mut *my_cache.ready_streams.lock());
        for key in ready {
            my_cache.blocking_keys.wake_ready(&key, |params| {
                let result =
                    my_cache.execute_multi_read(params.clone(), key.0, Some(update.write_clock));
                (!matches!(result, Value::Array(None))).then_some(result)
            });
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocha::ExpirePolicy;
    use crate::protocol::key::del::DelReq;
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::stream::xadd::XAddReq;
    use crate::protocol::stream::xread::{XReadId, XReadParams};
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::core::{MyValue, UpdateType};
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

    fn apply(cache: &MyCache, operation: Operation, db_number: u16, write_clock: u64) -> Value {
        let mut update_type = UpdateType::None;
        let mut update = Update {
            db_number,
            write_clock,
            update_type: &mut update_type,
        };
        do_request(cache, operation, &mut update, true)
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
        let notifications = cache.blocking_keys.clone();
        let first = notifications
            .register_with(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        let second = notifications
            .register_with(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        let mut other_db = notifications
            .register_with(vec![(1, key.clone())], None, read_params(&key))
            .unwrap();

        let result = apply(&cache, xadd(&key, AddId::Auto), 0, 1234);
        assert_eq!(result.encode(), b"$6\r\n1234-0\r\n");
        for registration in [first, second] {
            let result = tokio::time::timeout(Duration::from_secs(1), registration.wait())
                .await
                .expect("successful XADD must notify every reader");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert_eq!(reply.encode(), expected_reply("1234-0"));
        }
        assert!(other_db.cancel(), "another database must remain blocked");
        let entry = cache.databases[0].mocha.get_entry(&key).unwrap();
        let ValueObject::Stream(stream) = entry.value.data else {
            panic!("the stream must be stored before notifying readers");
        };
        assert_eq!(stream.read().last_generated_id(), StreamId::new(1234, 0));
    }

    #[test]
    fn failed_xadd_keeps_readers_registered() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let id = AddId::Explicit(StreamId::new(10, 0));
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10),
            Value::BulkString(Some(_))
        ));

        let mut reader = cache
            .blocking_keys
            .register_with(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, id), 0, 10),
            Value::Error(_)
        ));
        assert!(
            reader.cancel(),
            "a rejected stream ID must not wake readers"
        );

        cache.databases[0].mocha.insert_entry(
            key.clone(),
            MyValue::new(ValueObject::String(Bytes::from_static(b"wrong type"))),
            ExpirePolicy::Persistent,
        );
        let mut reader = cache
            .blocking_keys
            .register_with(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();
        assert!(matches!(
            apply(&cache, xadd(&key, AddId::Auto), 0, 11),
            Value::Error(_)
        ));
        assert!(reader.cancel(), "a type error must not wake readers");
    }

    #[tokio::test]
    async fn xadd_in_exec_and_lua_notifies_readers() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![xadd(&key, AddId::Auto)],
            })),
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script: "return redis.call('XADD', KEYS[1], '*', 'field', 'value')".to_owned(),
                numkeys: 1,
                keys: vec![key.clone()],
                args: vec![],
                proto: 2,
            })),
        ];
        for operation in operations {
            let mut params = read_params(&key);
            if let Some(entry) = cache.databases[0].mocha.get_entry(&key) {
                let ValueObject::Stream(stream) = entry.value.data else {
                    unreachable!()
                };
                params.ids[0] = XReadId::After(stream.read().last_generated_id());
            }
            let reader = cache
                .blocking_keys
                .register_with(vec![(0, key.clone())], None, params)
                .unwrap();
            let result = apply(&cache, operation, 0, 1234);
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            let result = tokio::time::timeout(Duration::from_secs(1), reader.wait())
                .await
                .expect("nested XADD must notify readers");
            let (ready_key, reply) = result.unwrap();
            assert_eq!(ready_key, (0, key.clone()));
            assert!(matches!(reply, Value::MapWithResp2 { .. }));
        }
    }

    #[tokio::test]
    async fn completed_xadd_reply_survives_a_following_del() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let reader = cache
            .blocking_keys
            .register_with(vec![(0, key.clone())], None, read_params(&key))
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234);
        let deleted = apply(
            &cache,
            Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
            0,
            1235,
        );
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
            .blocking_keys
            .register_with(vec![(0, key.clone())], None, params)
            .unwrap();

        apply(&cache, xadd(&key, AddId::Auto), 0, 1234);
        apply(&cache, xadd(&key, AddId::Auto), 0, 2001);

        let (_, reply) = tokio::time::timeout(Duration::from_secs(1), reader.wait())
            .await
            .expect("the later XADD must satisfy the same registration")
            .unwrap();
        assert_eq!(reply.encode(), expected_reply("2001-0"));
    }

    #[test]
    fn exec_and_lua_do_not_publish_intermediate_xadd_data() {
        let cache = MyCache::new(1).unwrap();
        let key = Bytes::from_static(b"stream");
        let operations = [
            Operation::Redis(RedisOperation::RedisExec(ExecParams {
                operations: vec![
                    xadd(&key, AddId::Auto),
                    Operation::Base(BaseOperation::Del(DelReq { key: key.clone() })),
                ],
            })),
            Operation::Redis(RedisOperation::RedisEval(EvalParams {
                script: "redis.call('XADD', KEYS[1], '*', 'field', 'value'); return redis.call('DEL', KEYS[1])".to_owned(),
                numkeys: 1,
                keys: vec![key.clone()],
                args: vec![],
                proto: 2,
            })),
        ];
        for operation in operations {
            let mut reader = cache
                .blocking_keys
                .register_with(vec![(0, key.clone())], None, read_params(&key))
                .unwrap();
            let result = apply(&cache, operation, 0, 1234);
            assert!(!matches!(result, Value::Error(_)), "{result:?}");
            assert!(cache.databases[0].mocha.get_entry(&key).is_none());
            assert!(
                reader.cancel(),
                "an intermediate XADD must not fulfill the read"
            );
        }
    }

    #[test]
    fn automatic_xadd_ids_use_the_replicated_clock_for_new_and_existing_streams() {
        let key = Bytes::from_static(b"stream");
        for _ in 0..2 {
            let cache = MyCache::new(1).unwrap();
            let initial = apply(&cache, xadd(&key, AddId::Auto), 0, 1234);
            let next = apply(&cache, xadd(&key, AddId::Auto), 0, 1234);
            assert_eq!(initial.encode(), b"$6\r\n1234-0\r\n");
            assert_eq!(next.encode(), b"$6\r\n1234-1\r\n");
        }
    }
}
