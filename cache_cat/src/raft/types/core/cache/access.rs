use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, IncrReq, LPushReq, SetReq};
use crate::raft::types::entry::request::AtomicRequest;
use crate::utils::parse_i64;
use moka::Entry;
use moka::ops::compute::{CompResult, Op};
use std::collections::LinkedList;
use std::sync::Arc;
use tokio::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

impl MyCache {
    pub async fn del(&self, del_req: DelReq, update: UpdateType<'_>) -> Value {
        let keys = (*del_req.keys).clone();
        let mut deleted = 0;

        match update {
            UpdateType::None => {
                for key in keys {
                    let existed = self.cache.remove(&key).await;
                    if existed.is_some() {
                        deleted += 1;
                    }
                }
                Value::Integer(deleted)
            }

            UpdateType::Snapshot(queue) => {
                for key in keys {
                    // 计算 version
                    let version = if let Some(entry) = self.cache.get(&key).await {
                        entry.version + 1
                    } else {
                        0
                    };

                    queue.push(AtomicRequest {
                        version,
                        request: BaseOperation::Del(DelReq {
                            keys: Arc::from(vec![key.clone()]), // 保持单 key 语义
                        }),
                    });

                    let existed = self.cache.remove(&key).await;
                    if existed.is_some() {
                        deleted += 1;
                    }
                }
                Value::Integer(deleted)
            }

            UpdateType::CAS(version) => {
                for key in keys {
                    if let Some(entry) = self.cache.get(&key).await {
                        if entry.version == version {
                            self.cache.remove(&key).await;
                            deleted += 1;
                        }
                    }
                }
                Value::Integer(deleted)
            }
        }
    }
    pub async fn set(&self, set_req: SetReq, update: UpdateType<'_>) {
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
                            if version == current_val.version {
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

    pub async fn incr(&self, incr_req: IncrReq, update: UpdateType<'_>) -> Value {
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
                                if value.version != version {
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
    pub async fn l_push(&self, l_push: LPushReq) -> Value {
        let result = self
            .cache
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
            .await;
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

    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
    pub async fn l_push_snapshot(&self, l_push: LPushReq, queue: &mut Vec<AtomicRequest>) -> Value {
        let result = self
            .cache
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
            .await;
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

    //成功就返回链表长度 失败返回错误内容 不存在就创建一个list
    pub async fn l_push_cas(&self, l_push: LPushReq, version: u32) -> Value {
        let result = self
            .cache
            .entry(l_push.key.clone())
            .and_compute_with(|maybe_entry| async move {
                match maybe_entry {
                    Some(entry) => {
                        let mut value = entry.into_value();
                        match &mut value.data {
                            ValueObject::List(data) => {
                                if value.version != version {
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
                        if version != 0 {
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
            .await;
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

    // todo 优化为字节编码
    //流式序列化和反序列化
    pub async fn dump_cache_to_writer<W>(&self, writer: &mut W) -> Result<(), io::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        for entry in self.cache.iter() {
            let (k_arc, v) = entry;
            let key_bytes = bincode2::serialize(&*k_arc)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let val_bytes = bincode2::serialize(&v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
        }

        writer.write_u64(0).await?;
        Ok(())
    }
    pub async fn load_cache_from_reader<R>(&self, reader: &mut R) -> Result<(), io::Error>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            let key_len = match reader.read_u64().await {
                Ok(v) => v as usize,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };
            if key_len == 0 {
                break;
            }

            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf).await?;

            let val_len = reader.read_u64().await? as usize;
            let mut val_buf = vec![0u8; val_len];
            reader.read_exact(&mut val_buf).await?;

            let key_vec: Vec<u8> = bincode2::deserialize(&key_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: MyValue = bincode2::deserialize(&val_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.cache.insert(Arc::new(key_vec), value).await;
        }

        Ok(())
    }
}
