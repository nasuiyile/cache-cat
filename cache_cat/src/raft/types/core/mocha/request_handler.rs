use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::{Operation, RedisOperation};

#[inline]
pub fn do_request(
    my_cache: &MyCache,
    operation: Operation,
    update: &mut Update,
    external: bool,
) -> Value {
    match operation {
        Operation::Read(read) => match read {
            ReadOperation::Exists(param) => my_cache.exists(param, update.db_number),
            ReadOperation::Get(param) => my_cache.get(param, update.db_number),
            ReadOperation::LRange(param) => my_cache.l_range(param, update.db_number),
            ReadOperation::MGet(param) => my_cache.m_get(param, update.db_number),
            ReadOperation::ZRange(param) => my_cache.z_range(param, update.db_number),
            ReadOperation::HGet(param) => my_cache.h_get(param, update.db_number),
            ReadOperation::SMembers(param) => my_cache.s_member(param, update.db_number),
            ReadOperation::HMGet(param) => my_cache.h_m_get(param, update.db_number),
            ReadOperation::GetBit(param) => my_cache.get_bit(param, update.db_number),
            ReadOperation::ZRangeByScore(param) => my_cache.z_range_by_score(param, update.db_number),
            ReadOperation::StrLen(param) => my_cache.str_len(param, update.db_number),
        },
        Operation::Base(base) => match base {
            BaseOperation::Empty => {
                for db in &my_cache.databases {
                    db.mocha.active_expire_cycle_blocking();
                }
                Value::ok()
            }
            BaseOperation::Set(param) => my_cache.set(param, update),
            BaseOperation::Expire(param) => my_cache.expire(param, update),
            BaseOperation::LPush(param) => my_cache.l_push(param, update),
            BaseOperation::Del(param) => my_cache.del(param, update),
            BaseOperation::Incr(param) => my_cache.incr(param, update),
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
        },
        Operation::Redis(redis) => match redis {
            RedisOperation::RedisDel(param) => my_cache.redis_del(param, update, external),
            RedisOperation::RedisSet(param) => my_cache.redis_set(param, update),
            RedisOperation::RedisMset(param) => my_cache.redis_mset(param, update, external),
            RedisOperation::RedisRename(param) => my_cache.redis_rename(param, update, external),
            RedisOperation::RedisRenameNx(param) => my_cache.redis_rename_nx(param, update, external),
            RedisOperation::RedisEval(param) => {
                if external {
                    let _exclusive_lock = my_cache.read_lock.write();
                }
                my_cache
                    .lua_env
                    .exec_lua(my_cache, &*param.script, &param.keys, &param.args, update)
                    .unwrap_or_else(|err| err.into())
            }
            RedisOperation::RedisExec(param) => {
                if external {
                    let _exclusive_lock = my_cache.read_lock.write();
                }
                let mut vec = Vec::new();
                for operation in param.operations {
                    vec.push(do_request(my_cache, operation, update, false));
                }
                Value::Array(Some(vec))
            }
            RedisOperation::RedisBLPop(req) => todo!(),
        },
    }
}
