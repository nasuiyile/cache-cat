use std::sync::Arc;
use moka::ops::compute::{CompResult, Op};
use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, IncrReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::parse_i64;

impl MyCache {
    pub async fn set(&self, set_req: SetReq, update: &mut UpdateType<'_>) {
        let mut value = match parse_i64(&set_req.value) {
            None => MyValue {
                data: ValueObject::String(set_req.value.clone()),
                expires_at: set_req.ex_time,
                version: 0,
            },
            Some(v) => MyValue {
                data: ValueObject::Int(v),
                expires_at: set_req.ex_time,
                version: 0,
            },
        };
        match update {
            UpdateType::None => {
                self.cache.insert(set_req.key.clone(), value).await;
            }
            UpdateType::Snapshot(queue) => {
                let key = set_req.key.clone();
                self.cache
                    .entry(key)
                    .and_upsert_with(|old_entry| {
                        let set_req = set_req.clone();
                        async move {
                            let old_version = if let Some(entry) = old_entry {
                                entry.into_value().version + 1
                            } else {
                                0
                            };
                            value.version = old_version;
                            queue.push(AtomicRequest {
                                version: value.version,
                                request: BaseOperation::Set(set_req),
                            });
                            value
                        }
                    })
                    .await;
            }
            UpdateType::CAS(version) => {
                let key = set_req.key.clone();
                self.cache
                    .entry(key)
                    .and_upsert_with(async move |maybe_entry| {
                        if let Some(entry) = maybe_entry {
                            let current_val = entry.value();
                            // 核心逻辑：只有传入的 version 与缓存中的 version 相同时才允许更新
                            if *version == current_val.version {
                                value
                            } else {
                                // 版本不匹配，直接返回旧值（即不更新）
                                current_val.clone()
                            }
                        } else {
                            let new_data = ValueObject::String(set_req.value.clone());
                            let ttl = set_req.ex_time;
                            MyValue {
                                data: new_data,
                                expires_at: ttl,
                                version: 1, // 初始版本
                            }
                        }
                    })
                    .await;
            }
        }
    }

    pub async fn incr(&self, incr_req: IncrReq, update: &mut UpdateType<'_>) -> Value {
        let key = incr_req.key.clone();
        let delta = incr_req.value;
        let result = match update {
            UpdateType::None => {
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                match &mut value.data {
                                    ValueObject::Int(n) => {
                                        *n += delta;
                                        Op::Put(value)
                                    }
                                    ValueObject::String(s) => {
                                        let num = match parse_i64(s) {
                                            None => {
                                                return Op::Nop;
                                            }
                                            Some(v) => v,
                                        };
                                        value.data = ValueObject::Int(num + delta);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                let value = MyValue {
                                    data: ValueObject::Int(delta),
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
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                queue.push(AtomicRequest {
                                    request: BaseOperation::Incr(incr_req.clone()),
                                    version: value.version + 1,
                                });
                                match &mut value.data {
                                    ValueObject::Int(n) => {
                                        *n += delta;
                                        Op::Put(value)
                                    }
                                    ValueObject::String(s) => {
                                        let num = match parse_i64(s) {
                                            None => {
                                                return Op::Nop;
                                            }
                                            Some(v) => v,
                                        };
                                        value.data = ValueObject::Int(num + delta);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                let value = MyValue {
                                    data: ValueObject::Int(delta),
                                    expires_at: 0,
                                    version: 0,
                                };
                                Op::Put(value)
                            }
                        }
                    })
                    .await
            }
            UpdateType::CAS(version) => {
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                if &value.version != version {
                                    return Op::Nop;
                                }
                                value.version += 1;
                                match &mut value.data {
                                    ValueObject::Int(n) => {
                                        *n += delta;
                                        Op::Put(value)
                                    }
                                    ValueObject::String(s) => {
                                        let num = match parse_i64(s) {
                                            None => {
                                                return Op::Nop;
                                            }
                                            Some(v) => v,
                                        };
                                        value.data = ValueObject::Int(num + delta);
                                        Op::Put(value)
                                    }
                                    _ => Op::Nop,
                                }
                            }
                            None => {
                                let value = MyValue {
                                    data: ValueObject::Int(delta),
                                    expires_at: 0,
                                    version: 0,
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
                ValueObject::Int(n) => Value::Integer(n),
                _ => Value::Error("Key exists but is not an Integer".to_string()),
            },
            CompResult::StillNone(_) => Value::Error("Unexpected: key not found".to_string()),
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }
    //如果不是string就报错，如果是string就append，如果没有值就创建一个
    pub async fn append(&self, key: Arc<Vec<u8>>, suffix: Arc<Vec<u8>>) -> Result<u32, String> {
        let result = self
            .cache
            .entry(key)
            .and_compute_with(|maybe_entry| {
                let suffix = suffix.clone();
                async move {
                    match maybe_entry {
                        Some(entry) => {
                            let mut value = entry.into_value();
                            match &mut value.data {
                                ValueObject::String(data_arc) => {
                                    let data = Arc::make_mut(data_arc);
                                    data.extend_from_slice(&suffix);
                                    value.version += 1;
                                    Op::Put(value)
                                }
                                _ => {
                                    // 这里不能返回 Err，只能 Nop 或 Put
                                    Op::Nop
                                }
                            }
                        }
                        None => Op::Put(MyValue {
                            data: ValueObject::String(suffix.clone()),
                            expires_at: 0,
                            version: 1,
                        }),
                    }
                }
            })
            .await;

        //  在这里统一解析返回值
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => match entry.into_value().data {
                ValueObject::String(data_arc) => Ok(data_arc.len() as u32),
                _ => Err("Key exists but is not a String".to_string()),
            },
            CompResult::StillNone(_) => {
                // 理论不会发生（因为我们 Put 了）
                Err("Unexpected: key not found".to_string())
            }
            CompResult::Removed(_) => Err("Unexpected: value removed".to_string()),
        }
    }
}
