use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::size_estimate::{
    estimate_hash_table_usage, estimated_bytes_heap_usage, sampled_total,
};
use bytes::Bytes;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

/// A score boundary used by the extended ZRANGE command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

/// A lexicographical boundary used by the extended ZRANGE command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LexBound {
    NegativeInfinity,
    PositiveInfinity,
    Value { value: Bytes, exclusive: bool },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SortedSet {
    /// 按 (score, member) 排序。
    ///
    /// Redis Sorted Set 在 score 相同时，按照 member 的字典序排列。
    tree: BTreeSet<(OrderedFloat<f64>, Bytes)>,

    /// member -> score
    ///
    /// 用于 O(1) 查询 member 是否存在以及它当前的 score。
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
            let old_score = self.hash.get(&member).copied();
            let exists = old_score.is_some();

            // NX: 只添加不存在的 member
            if req.nx && exists {
                continue;
            }

            // XX: 只更新已经存在的 member
            if req.xx && !exists {
                continue;
            }

            if let Some(old_score) = old_score {
                // GT: 新 score 必须大于旧 score
                if req.gt && score <= old_score {
                    continue;
                }

                // LT: 新 score 必须小于旧 score
                if req.lt && score >= old_score {
                    continue;
                }

                // score 真正发生改变才需要修改 tree/hash
                if old_score != score {
                    self.tree.remove(&(OrderedFloat(old_score), member.clone()));

                    self.tree.insert((OrderedFloat(score), member.clone()));

                    self.hash.insert(member, score);

                    changed += 1;
                }
            } else {
                // 新 member
                self.tree.insert((OrderedFloat(score), member.clone()));

                self.hash.insert(member, score);

                added += 1;
                changed += 1;
            }
        }

        if req.ch { changed } else { added }
    }

    /// 按 rank 返回成员。
    ///
    /// start/stop 都是闭区间，并支持 Redis 风格负数索引：
    ///
    /// - 0 = 第一个
    /// - 1 = 第二个
    /// - -1 = 最后一个
    /// - -2 = 倒数第二个
    pub fn zrange(&self, start: i64, stop: i64) -> Vec<(Bytes, f64)> {
        self.zrange_rank(start, stop, false)
    }

    /// ZRANGE by rank, optionally in reverse order.
    pub fn zrange_rank(&self, start: i64, stop: i64, reverse: bool) -> Vec<(Bytes, f64)> {
        let len = self.tree.len() as i64;

        if len == 0 {
            return Vec::new();
        }

        // Use a wider intermediate so i64::MIN cannot overflow when adding
        // a negative index to the collection length.
        let mut start_idx = if start < 0 {
            len as i128 + start as i128
        } else {
            start as i128
        };

        let mut stop_idx = if stop < 0 {
            len as i128 + stop as i128
        } else {
            stop as i128
        };

        // Redis 语义：
        // start 过小则修正到 0
        if start_idx < 0 {
            start_idx = 0;
        }

        // stop 超出末尾则修正到 len - 1
        if stop_idx >= len as i128 {
            stop_idx = len as i128 - 1;
        }

        // stop 仍然 < 0，说明整个范围都在集合之前
        if stop_idx < 0 {
            return Vec::new();
        }

        if start_idx >= len as i128 || start_idx > stop_idx {
            return Vec::new();
        }

        let start_idx = start_idx as usize;
        let count = (stop_idx - start_idx as i128 + 1) as usize;

        if reverse {
            self.tree
                .iter()
                .rev()
                .skip(start_idx)
                .take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            self.tree
                .iter()
                .skip(start_idx)
                .take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }

    /// ZRANGE BYSCORE. `first` and `second` retain the command order: in
    /// reverse mode the first boundary is the high score and the second is
    /// the low score.
    pub fn zrange_score(
        &self,
        first: &ScoreBound,
        second: &ScoreBound,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<(Bytes, f64)> {
        if self.tree.is_empty() {
            return Vec::new();
        }

        let (lower, upper) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        let lower_matches = |score: f64| {
            if lower.exclusive {
                score > lower.value
            } else {
                score >= lower.value
            }
        };
        let upper_matches = |score: f64| {
            if upper.exclusive {
                score < upper.value
            } else {
                score <= upper.value
            }
        };

        if let Some((offset, _)) = limit {
            if offset < 0 {
                return Vec::new();
            }
        }
        if matches!(limit, Some((_, 0))) {
            return Vec::new();
        }

        if reverse {
            // The tuple tree has no finite member sentinel for an inclusive
            // upper score, so start from the end and discard scores above the
            // requested upper bound before taking the bounded range.
            let iter = self
                .tree
                .iter()
                .rev()
                .skip_while(|(score, _)| !upper_matches(score.0))
                .take_while(|(score, _)| lower_matches(score.0))
                .map(|(score, member)| (member.clone(), score.0));
            collect_zrange_limit(iter, limit)
        } else {
            // Bytes::new() is the smallest member for a given score, which
            // lets us avoid scanning entries below the lower score.
            let start = (OrderedFloat(lower.value), Bytes::new());
            let iter = self
                .tree
                .range(start..)
                .filter(|(score, _)| lower_matches(score.0))
                .take_while(|(score, _)| upper_matches(score.0))
                .map(|(score, member)| (member.clone(), score.0));
            collect_zrange_limit(iter, limit)
        }
    }

    /// ZRANGE BYLEX. Redis defines this mode for sorted sets whose members
    /// have the same score. We still compare raw member bytes, as Redis does,
    /// and keep the scan deterministic for sets with mixed scores.
    pub fn zrange_lex(
        &self,
        first: &LexBound,
        second: &LexBound,
        reverse: bool,
        limit: Option<(i64, i64)>,
    ) -> Vec<(Bytes, f64)> {
        let (lower, upper) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        let in_range = |member: &Bytes| {
            let lower_ok = match lower {
                LexBound::NegativeInfinity => true,
                LexBound::PositiveInfinity => false,
                LexBound::Value { value, exclusive } => {
                    if *exclusive {
                        member > value
                    } else {
                        member >= value
                    }
                }
            };
            let upper_ok = match upper {
                LexBound::NegativeInfinity => false,
                LexBound::PositiveInfinity => true,
                LexBound::Value { value, exclusive } => {
                    if *exclusive {
                        member < value
                    } else {
                        member <= value
                    }
                }
            };
            lower_ok && upper_ok
        };

        if reverse {
            let iter = self
                .tree
                .iter()
                .rev()
                .filter(|(_, member)| in_range(member))
                .map(|(score, member)| (member.clone(), score.0));
            collect_zrange_limit(iter, limit)
        } else {
            let iter = self
                .tree
                .iter()
                .filter(|(_, member)| in_range(member))
                .map(|(score, member)| (member.clone(), score.0));
            collect_zrange_limit(iter, limit)
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hash.len()
    }

    /// 统计 score 在指定范围中的 member 数量。
    ///
    /// min_exclusive:
    /// - false => score >= min
    /// - true  => score > min
    ///
    /// max_exclusive:
    /// - false => score <= max
    /// - true  => score < max
    pub fn zcount(&self, min: f64, max: f64, min_exclusive: bool, max_exclusive: bool) -> i64 {
        if self.tree.is_empty() {
            return 0;
        }
        if min > max {
            return 0;
        }
        self.tree
            .iter()
            .filter(|(score, _)| {
                let score = score.0;
                let min_ok = if min_exclusive {
                    score > min
                } else {
                    score >= min
                };
                let max_ok = if max_exclusive {
                    score < max
                } else {
                    score <= max
                };
                min_ok && max_ok
            })
            .count() as i64
    }
    /// ZRANGEBYSCORE
    ///
    /// min/max 当前为闭区间：
    ///
    ///     min <= score <= max
    ///
    /// limit:
    ///
    ///     Some((offset, count))
    pub fn zrangebyscore(
        &self,
        min: f64,
        max: f64,
        limit: Option<(usize, usize)>,
    ) -> Vec<(Bytes, f64)> {
        if self.tree.is_empty() {
            return Vec::new();
        }
        if min > max {
            return Vec::new();
        }
        let skip_count = limit.map(|(offset, _)| offset).unwrap_or(0);
        let take_count = limit.map(|(_, count)| count).unwrap_or(usize::MAX);
        if take_count == 0 {
            return Vec::new();
        }
        /*
         * BTreeSet 的 key 是：
         *
         *     (score, member)
         *
         * Bytes::new() 是所有非空 Bytes 的最小值，因此可以从：
         *
         *     (min, "")
         *
         * 开始 range。
         *
         * 不能写：
         *
         *     ..=(max, Bytes::new())
         *
         * 因为这会漏掉：
         *
         *     (max, "abc")
         *     (max, "xyz")
         *
         * 所以这里只设置下界，然后通过 take_while 控制 score 上界。
         */
        let start = (OrderedFloat(min), Bytes::new());
        self.tree
            .range(start..)
            .take_while(|(score, _)| score.0 <= max)
            .skip(skip_count)
            .take(take_count)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }

    /// 删除指定成员。
    ///
    /// 时间复杂度：
    ///
    /// O(M * log N)
    ///
    /// M = members 数量
    pub fn zrem(&mut self, members: &[Bytes]) -> i64 {
        let mut removed = 0i64;
        for member in members {
            if let Some(score) = self.hash.remove(member) {
                self.tree.remove(&(OrderedFloat(score), member.clone()));
                removed += 1;
            }
        }
        removed
    }

    /// ZINCRBY
    ///
    /// member 不存在时，相当于从 0 开始增加。
    pub fn zincrby(&mut self, member: Bytes, increment: f64) -> Option<f64> {
        let old_score = self.hash.get(&member).copied();
        let new_score = old_score.unwrap_or(0.0) + increment;
        // Redis 不允许 NaN score。
        if new_score.is_nan() {
            return None;
        }
        if let Some(old_score) = old_score {
            // 如果 score 没变化，就不需要重新插入 tree。
            if old_score == new_score {
                return Some(new_score);
            }
            self.tree.remove(&(OrderedFloat(old_score), member.clone()));
        }
        self.tree.insert((OrderedFloat(new_score), member.clone()));
        self.hash.insert(member, new_score);
        Some(new_score)
    }

    /// ZSCORE
    #[inline]
    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.hash.get(member).copied()
    }

    /// ZRANK
    ///
    /// 当前复杂度仍然是 O(N)。
    pub fn zrank(&self, member: &Bytes) -> Option<i64> {
        let score = self.hash.get(member).copied()?;
        /*
         * 已经知道 score，所以不需要像原代码一样：
         *
         * self.tree.iter().position(|(_, m)| m == member)
         *
         * 从逻辑上搜索 member。
         *
         * 不过 std::collections::BTreeSet 不提供 order-statistics，
         * 所以获取 rank 本身仍需要迭代，复杂度 O(N)。
         */
        let key = (OrderedFloat(score), member.clone());
        self.tree
            .iter()
            .position(|item| item == &key)
            .map(|rank| rank as i64)
    }

    /// ZREVRANK
    ///
    /// 当前复杂度 O(N)。
    pub fn zrevrank(&self, member: &Bytes) -> Option<i64> {
        let score = self.hash.get(member).copied()?;
        let key = (OrderedFloat(score), member.clone());
        self.tree
            .iter()
            .rev()
            .position(|item| item == &key)
            .map(|rank| rank as i64)
    }

    /// 检查集合是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.hash.is_empty()
    }

    /// ZPOPMIN
    pub fn zpop_min(&mut self, count: Option<usize>) -> Vec<(Bytes, f64)> {
        let count = match count {
            None => 1,
            Some(0) => return Vec::new(),
            Some(count) => count,
        };
        let mut values = Vec::with_capacity(count.min(self.tree.len()));
        for _ in 0..count {
            let (score, member) = match self.tree.pop_first() {
                Some(value) => value,
                None => break,
            };
            self.hash.remove(&member);
            values.push((member, score.0));
        }
        values
    }

    /// 估算树、哈希表和成员 payload；不包含 SortedSet 的内联大小。
    /// SAMPLES 0 全量遍历，tree/hash 共享的成员字节只计算一次。
    pub fn estimated_heap_usage(&self, samples: usize) -> usize {
        let hash_memory = estimate_hash_table_usage::<(Bytes, f64)>(self.hash.capacity());

        // BTreeSet 不公开节点容量，每项暂按值大小加两个指针估算。
        let tree_entry_size = size_of::<(OrderedFloat<f64>, Bytes)>()
            .saturating_add(size_of::<usize>().saturating_mul(2));
        let tree_memory = self.tree.len().saturating_mul(tree_entry_size);
        let member_payload = sampled_total(
            self.tree
                .iter()
                .map(|(_, member)| estimated_bytes_heap_usage(member)),
            self.tree.len(),
            samples,
        );

        hash_memory
            .saturating_add(tree_memory)
            .saturating_add(member_payload)
    }
}

fn collect_zrange_limit<I, T>(iter: I, limit: Option<(i64, i64)>) -> Vec<T>
where
    I: Iterator<Item = T>,
{
    match limit {
        None => iter.collect(),
        Some((offset, _)) if offset < 0 => Vec::new(),
        Some((_, 0)) => Vec::new(),
        Some((offset, count)) => {
            let offset = offset as usize;
            if count < 0 {
                iter.skip(offset).collect()
            } else {
                iter.skip(offset).take(count as usize).collect()
            }
        }
    }
}
