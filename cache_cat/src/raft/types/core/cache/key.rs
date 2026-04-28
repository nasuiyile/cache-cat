use crate::raft::types::core::cache::moka::{MyCache, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq};
use crate::raft::types::entry::request::AtomicRequest;
use std::sync::Arc;

impl MyCache {
    pub async fn del(&self, del_req: DelReq, update: &mut UpdateType<'_>) -> Value {
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
                        if entry.version == *version {
                            self.cache.remove(&key).await;
                            deleted += 1;
                        }
                    }
                }
                Value::Integer(deleted)
            }
        }
    }
}
