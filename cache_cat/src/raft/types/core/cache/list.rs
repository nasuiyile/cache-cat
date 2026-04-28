use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, LPushReq};
use crate::raft::types::entry::request::AtomicRequest;
use moka::ops::compute::{CompResult, Op};
use std::collections::LinkedList;

impl MyCache {
    pub async fn l_push(&self, l_push: LPushReq, update: &mut UpdateType<'_>) -> Value {
        let result = match update {
            UpdateType::None => {
                self.cache
                    .entry(l_push.key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                match &mut value.data {
                                    ValueObject::List(data) => {
                                        value.version;
                                        data.push_front(l_push.value);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                let value = MyValue {
                                    data: ValueObject::List(LinkedList::from([l_push.value])),
                                    expires_at: 0,
                                    version: 0,
                                };
                                Op::Put(value)
                            }
                        }
                    })
                    .await
            }
            UpdateType::Snapshot(queue) => {
                //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
                self.cache
                    .entry(l_push.key.clone())
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                match &mut value.data {
                                    ValueObject::List(data) => {
                                        queue.push(AtomicRequest {
                                            version: value.version,
                                            request: BaseOperation::LPush(l_push.clone()),
                                        });
                                        value.version += 1;
                                        data.push_front(l_push.value);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                queue.push(AtomicRequest {
                                    version: 1,
                                    request: BaseOperation::LPush(l_push.clone()),
                                });
                                let value = MyValue {
                                    data: ValueObject::List(LinkedList::from([l_push.value])),
                                    expires_at: 0,
                                    version: 1,
                                };
                                Op::Put(value)
                            }
                        }
                    })
                    .await
            }
            UpdateType::CAS(version) => {
                self.cache
                    .entry(l_push.key.clone())
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                match &mut value.data {
                                    ValueObject::List(data) => {
                                        if value.version != *version {
                                            return Op::Nop;
                                        }
                                        value.version += 1;
                                        data.push_front(l_push.value);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                if *version != 0 {
                                    //理论上不会出现
                                    tracing::error!("CAS failed: operation not found");
                                }
                                let value = MyValue {
                                    data: ValueObject::List(LinkedList::from([l_push.value])),
                                    expires_at: 0,
                                    version: 1,
                                };
                                Op::Put(value)
                            }
                        }
                    })
                    .await
            }
        };

        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::List(data_arc) => Value::Integer(data_arc.len() as i64),
                _ => Value::Error("Key exists but is not a List".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Value::Error("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }
}
