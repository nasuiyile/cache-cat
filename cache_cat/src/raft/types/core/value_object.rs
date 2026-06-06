use crate::raft::types::entry::bae_operation::ZAddReq;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortedSet {
    tree: BTreeMap<(OrderedFloat<f64>, Arc<Vec<u8>>), ()>,
    hash: HashMap<Arc<Vec<u8>>, f64>,
}

impl SortedSet {
    pub fn new() -> Self {
        SortedSet {
            tree: BTreeMap::new(),
            hash: HashMap::new(),
        }
    }
    pub fn zadd(&mut self, req: ZAddReq) -> i64 {
        let mut added = 0;
        let mut changed = 0;

        for (member, score) in req.members {
            let old_score = self.hash.get(&member).cloned();
            let exists = old_score.is_some();

            // 1. 处理 NX (只新增) / XX (只更新)
            if req.nx && exists {
                continue;
            }
            if req.xx && !exists {
                continue;
            }

            if let Some(old_s) = old_score {
                // 2. 处理 GT / LT
                if req.gt && score <= old_s {
                    continue;
                }
                if req.lt && score >= old_s {
                    continue;
                }

                // 3. 执行更新
                if old_s != score {
                    // 先从 tree 中移除旧的排序节点
                    self.tree.remove(&(OrderedFloat(old_s), member.clone()));
                    // 插入新的排序节点
                    self.tree.insert((OrderedFloat(score), member.clone()), ());
                    // 更新哈希表
                    self.hash.insert(member.clone(), score);
                    changed += 1;
                }
            } else {
                // 4. 执行新增
                self.tree.insert((OrderedFloat(score), member.clone()), ());
                self.hash.insert(member.clone(), score);
                added += 1;
                changed += 1;
            }
        }

        if req.ch { changed } else { added }
    }

    pub fn zrange(&self, start: i64, stop: i64, with_scores: bool) -> Vec<Vec<u8>> {
        let len = self.hash.len() as i64;
        if len == 0 {
            return vec![];
        }
        // 1. 处理 Redis 负数索引逻辑
        let mut start_idx = if start < 0 { len + start } else { start };
        let mut stop_idx = if stop < 0 { len + stop } else { stop };
        // 边界修正
        if start_idx < 0 {
            start_idx = 0;
        }
        if stop_idx >= len {
            stop_idx = len - 1;
        }
        if start_idx > stop_idx || start_idx >= len {
            return vec![];
        }
        let count = (stop_idx - start_idx + 1) as usize;
        // 2. 预分配空间以提高性能
        // 如果带分数，空间翻倍
        let result_capacity = if with_scores { count * 2 } else { count };
        let mut result = Vec::with_capacity(result_capacity);

        // 3. 迭代 BTreeMap 提取数据
        // tree 的顺序已经是 (Score, Member) 排序好的
        let range_iter = self.tree.keys().skip(start_idx as usize).take(count);

        for (score, member) in range_iter {
            // 插入成员
            result.push((**member).clone());

            // 如果需要分数，将 f64 转换为字符串字节
            if with_scores {
                let s = score.0.to_string();
                result.push(s.into_bytes());
            }
        }
        result
    }
    pub fn zrangebyscore(
        &self,
        min: f64,
        max: f64,
        with_scores: bool,
        limit: Option<(usize, usize)>,
    ) -> Vec<Vec<u8>> {
        if self.tree.is_empty() {
            return vec![];
        }

        let min_score = OrderedFloat(min);
        let max_score = OrderedFloat(max);

        // 直接遍历整个 BTreeMap，在迭代中进行分数过滤
        // 这样避免了边界值问题，且不需要 collect
        let skip_count = limit.map(|(offset, _)| offset).unwrap_or(0);
        let take_count = limit.map(|(_, count)| count).unwrap_or(usize::MAX);

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut taken = 0;

        for ((score, member), _) in self.tree.range(
            (min_score, Arc::new(vec![]))..=(max_score, Arc::new(vec![]))
        ) {
            if skipped < skip_count {
                skipped += 1;
                continue;
            }

            if taken >= take_count {
                break;
            }

            result.push((**member).clone());
            if with_scores {
                result.push(score.0.to_string().into_bytes());
            }
            taken += 1;
        }

        result
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HashValue {
    Str(Arc<Vec<u8>>),
    Int(i64),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),
    String(Arc<Vec<u8>>),
    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>),
    #[serde(with = "mutex_hashmap_serde")]
    Hash(Arc<Mutex<HashMap<Arc<Vec<u8>>, HashValue>>>),
    #[serde(with = "mutex_zset_serde")]
    ZSet(Arc<Mutex<SortedSet>>),
    #[serde(with = "mutex_hashset_serde")]
    Set(Arc<Mutex<HashSet<Arc<Vec<u8>>>>>),
}

// 通用序列化宏
macro_rules! impl_mutex_serde {
    ($mod_name:ident, $inner_type:ty) => {
        mod $mod_name {
            use super::*;
            use serde::de::Deserializer;
            use serde::{Deserialize, Serialize};

            pub fn serialize<S>(
                data: &Arc<Mutex<$inner_type>>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let guard = data.lock();
                guard.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Mutex<$inner_type>>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$inner_type>::deserialize(deserializer)?;
                Ok(Arc::new(Mutex::new(value)))
            }
        }
    };
}

impl_mutex_serde!(mutex_vecdeque_serde, VecDeque<Arc<Vec<u8>>>);
impl_mutex_serde!(mutex_hashmap_serde, HashMap<Arc<Vec<u8>>, HashValue>);
impl_mutex_serde!(mutex_zset_serde, SortedSet);
impl_mutex_serde!(mutex_hashset_serde, HashSet<Arc<Vec<u8>>>);
