use crate::raft::types::core::mocha::mocha::{MyCache, Update};
use crate::raft::types::core::response_value::Value;
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
    }
}

#[inline]
pub fn do_request(
    my_cache: &MyCache,
    operation: Operation,
    update: &mut Update,
    external: bool, //用来防止多次加锁
) -> Value {
    match operation {
        Operation::Read(read) => read_request(my_cache, read, update.db_number, None),
        Operation::Base(base) => base_request(my_cache, base, update),
        Operation::Redis(redis) => match redis {
            RedisOperation::RedisDel(param) => my_cache.redis_del(param, update, external),
            RedisOperation::RedisSet(param) => my_cache.redis_set(param, update),
            RedisOperation::RedisSetNx(param) => my_cache.redis_setnx(param, update),
            RedisOperation::RedisSetEx(param) => my_cache.redis_setex(param, update),
            RedisOperation::RedisPSetEx(param) => my_cache.redis_psetex(param, update),
            RedisOperation::RedisGetSet(param) =>my_cache.redis_get_set(param, update),
            RedisOperation::RedisMset(param) => my_cache.redis_mset(param, update, external),
            RedisOperation::RedisRename(param) => my_cache.redis_rename(param, update, external),
            RedisOperation::RedisRenameNx(param) => {
                my_cache.redis_rename_nx(param, update, external)
            }
            RedisOperation::RedisEval(param) => {
                if external {
                    let _exclusive_lock = my_cache.read_lock.write();
                }
                my_cache
                    .lua_env
                    .exec_lua(my_cache, &param.script, &param.keys, &param.args, update)
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
            },
        },
    }
}
