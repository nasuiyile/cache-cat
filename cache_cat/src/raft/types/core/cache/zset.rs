use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::zset::zrange::ZRangeParams;
use crate::protocol::zset::zrangegetscore::ZRangeByScoreParams;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::SortedSet;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::bae_operation::{BaseOperation, ZAddReq};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::Arc;

impl ComputeCommand for ZAddReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::ZAdd(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ZSet(zset) => {
                let changed_count = zset.lock().zadd(self);
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(changed_count),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error("zadd: key is not a zset".to_string()),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let mut set = SortedSet::new();
        let changed_count = set.zadd(self);
        (
            MochaOperation::Insert {
                value: MyValue::new(ZSet(Arc::new(Mutex::new(set)))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Integer(changed_count),
        )
    }
}

impl ReadCommand for ZRangeByScoreParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ZSet(list) => {
                    let zset = list.lock();
                    let res = zset.zrangebyscore(self.min, self.max, self.with_scores, self.limit);
                    let mut vec = Vec::with_capacity(res.len());
                    for v in res {
                        vec.push(Value::BulkString(Some(v)));
                    }
                    Value::Array(Some(vec))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}
impl ReadCommand for ZRangeParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])), // 空集合返回空数组
            Some(v) => match v.data {
                ZSet(list) => {
                    let res = list.lock().zrange(self.start, self.stop, self.with_scores);

                    if self.with_scores {
                        // 使用 Pairs 类型
                        let mut pairs = Vec::with_capacity(res.len() / 2);
                        let mut iter = res.into_iter();
                        while let Some(member) = iter.next() {
                            if let Some(score_bytes) = iter.next() {
                                let score = String::from_utf8_lossy(&score_bytes);
                                // 尝试解析为数字
                                let score_value = if let Ok(num) = score.parse::<i64>() {
                                    Value::Integer(num)
                                } else if let Ok(num) = score.parse::<f64>() {
                                    Value::BulkString(Some(score_bytes))
                                } else {
                                    Value::BulkString(Some(score_bytes))
                                };

                                pairs.push((Value::BulkString(Some(member)), score_value));
                            }
                        }
                        Value::Pairs(pairs)
                    } else {
                        // 只有成员，返回普通数组
                        let mut vec = Vec::with_capacity(res.len());
                        for member in res {
                            vec.push(Value::BulkString(Some(member)));
                        }
                        Value::Array(Some(vec))
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}

impl MyCache {
    pub fn z_range_by_score(
        &self,
        params: ZRangeByScoreParams,
        db_number: u16,
        read_clock: Option<u64>,
    ) -> Value {
        self.execute_read(params, db_number, read_clock)
    }

    pub fn z_range(&self, params: ZRangeParams, db_number: u16, read_clock: Option<u64>) -> Value {
        self.execute_read(params, db_number, read_clock)
    }
    pub fn z_add(&self, zadd: ZAddReq, update: &mut Update) -> Value {
        self.execute_compute(zadd, update)
    }
}
