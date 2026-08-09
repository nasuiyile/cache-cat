use crate::protocol::zset::zadd::ZAddReq;
use bytes::Bytes;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SortedSet {
    tree: BTreeSet<(OrderedFloat<f64>, Bytes)>,
    hash: HashMap<Bytes, f64>,
}

impl SortedSet {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn zadd(&mut self, req: ZAddReq) -> i64 {
        let mut added = 0;
        let mut changed = 0;

        for (member, score) in req.members {
            let old_score = self.hash.get(&member).cloned();
            let exists = old_score.is_some();

            // 1. Processing NX (new only) / XX (updated only)
            if req.nx && exists {
                continue;
            }
            if req.xx && !exists {
                continue;
            }

            if let Some(old_s) = old_score {
                // 2. Processing GT / LT
                if req.gt && score <= old_s {
                    continue;
                }
                if req.lt && score >= old_s {
                    continue;
                }

                // 3. perform updates
                if old_s != score {
                    // Remove the old sorting nodes from the tree first
                    self.tree.remove(&(OrderedFloat(old_s), member.clone()));
                    // Insert a new sorting node
                    self.tree.insert((OrderedFloat(score), member.clone()));
                    // Update Hash Table
                    self.hash.insert(member.clone(), score);
                    changed += 1;
                }
            } else {
                // 4. Execute addition
                self.tree.insert((OrderedFloat(score), member.clone()));
                self.hash.insert(member.clone(), score);
                added += 1;
                changed += 1;
            }
        }

        if req.ch { changed } else { added }
    }

    pub fn zrange(&self, start: i64, stop: i64, with_scores: bool) -> Vec<Bytes> {
        let len = self.hash.len() as i64;
        if len == 0 {
            return vec![];
        }
        // 1. Handling Redis Negative Index Logic
        let mut start_idx = if start < 0 { len + start } else { start };
        let mut stop_idx = if stop < 0 { len + stop } else { stop };
        // Boundary correction
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
        // 2. Pre allocate space to improve performance
        // If scores are included, the space doubles
        let result_capacity = if with_scores { count * 2 } else { count };
        let mut result = Vec::with_capacity(result_capacity);

        // 3. Iterate BTreeMap to extract data
        // The order of the tree is already sorted by (Score, Member)
        let range_iter = self.tree.iter().skip(start_idx as usize).take(count);

        for (score, member) in range_iter {
            // Insert member
            result.push(member.clone());

            // If scores are required, convert f64 to string bytes
            if with_scores {
                result.push(score.0.to_string().into())
            }
        }
        result
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hash.len()
    }

    pub fn zcount(&self, min: f64, max: f64, min_exclusive: bool, max_exclusive: bool) -> i64 {
        use std::ops::Bound;

        if self.tree.is_empty() {
            return 0;
        }

        let start = if min_exclusive {
            Bound::Excluded((OrderedFloat(min), Bytes::from(vec![0xff])))
        } else {
            Bound::Included((OrderedFloat(min), Bytes::new()))
        };

        let end = if max_exclusive {
            Bound::Excluded((OrderedFloat(max), Bytes::new()))
        } else {
            Bound::Included((OrderedFloat(max), Bytes::from(vec![0xff])))
        };

        self.tree.range((start, end)).count() as i64
    }

    pub fn zrangebyscore(
        &self,
        min: f64,
        max: f64,
        with_scores: bool,
        limit: Option<(usize, usize)>,
    ) -> Vec<Bytes> {
        if self.tree.is_empty() {
            return vec![];
        }

        let min_score = OrderedFloat(min);
        let max_score = OrderedFloat(max);

        // Traverse the entire BTreeMap directly and perform score filtering during iterations
        // This avoids boundary value problems and does not require collection
        let skip_count = limit.map(|(offset, _)| offset).unwrap_or(0);
        let take_count = limit.map(|(_, count)| count).unwrap_or(usize::MAX);

        let mut result = Vec::new();
        let mut skipped = 0;
        let mut taken = 0;

        for (score, member) in self
            .tree
            .range((min_score, Bytes::new())..=(max_score, Bytes::new()))
        {
            if skipped < skip_count {
                skipped += 1;
                continue;
            }

            if taken >= take_count {
                break;
            }

            result.push(member.clone());
            if with_scores {
                result.push(score.0.to_string().into())
            }
            taken += 1;
        }

        result
    }

    /// 删除指定的成员，返回实际删除的数量
    /// 时间复杂度：O(M * log(N))，M 是要删除的成员数
    pub fn zrem(&mut self, members: &[Bytes]) -> i64 {
        let mut removed = 0i64;

        for member in members {
            if let Some(score) = self.hash.remove(member) {
                // 从 tree 中删除（tree 的 key 是 (score, member)）
                self.tree.remove(&(OrderedFloat(score), member.clone()));
                removed += 1;
            }
        }

        removed
    }

    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.hash.get(member).cloned()
    }

    pub fn zrank(&self, member: &Bytes) -> Option<i64> {
        let member_score = self.hash.get(member)?;

        for (index, (score, current_member)) in self.tree.iter().enumerate() {
            if score.0 == *member_score && current_member == member {
                return Some(index as i64);
            }
        }

        None
    }

    /// 检查集合是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.hash.is_empty()
    }

    pub fn zpop_min(&mut self, count: Option<usize>) -> Vec<(Bytes, f64)> {
        let count = match count {
            None => 1,
            Some(0) => return Vec::default(),
            Some(count) => count,
        };
        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            let Some((score, value)) = self.tree.pop_first() else {
                break;
            };

            self.hash.remove(&value);

            values.push((value, score.0));
        }

        values
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HashValue {
    Str(Bytes),
    Int(i64),
}

impl HashValue {
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            HashValue::Str(str) => str.clone(),
            HashValue::Int(int) => int.to_string().into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),
    String(Bytes),
    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Bytes>>>),
    #[serde(with = "mutex_hashmap_serde")]
    Hash(Arc<Mutex<HashMap<Bytes, HashValue>>>),
    #[serde(with = "mutex_zset_serde")]
    ZSet(Arc<Mutex<SortedSet>>),
    #[serde(with = "mutex_hashset_serde")]
    Set(Arc<Mutex<HashSet<Bytes>>>),
}

// Universal serialization macro
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

impl_mutex_serde!(mutex_vecdeque_serde, VecDeque<Bytes>);
impl_mutex_serde!(mutex_hashmap_serde, HashMap<Bytes, HashValue>);
impl_mutex_serde!(mutex_zset_serde, SortedSet);
impl_mutex_serde!(mutex_hashset_serde, HashSet<Bytes>);
