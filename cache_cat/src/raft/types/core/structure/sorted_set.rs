use super::indexed_skiplist::{IndexedSkipList, SortedSkipListBuilder};
use super::scan_map::ScanMap;
use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::size_estimate::{estimated_bytes_heap_usage, sampled_total};
use bytes::Bytes;
use ordered_float::OrderedFloat;
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

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

#[derive(Clone, Debug, Default)]
pub struct SortedSet {
    /// 按 (score, member) 排序。
    ///
    /// Redis Sorted Set 在 score 相同时，按照 member 的字典序排列。
    tree: IndexedSkipList,

    /// member -> score
    ///
    /// 用于 O(1) 查询 member 是否存在以及它当前的 score。
    hash: ScanMap<Bytes, f64>,
}

// Persist logical entries once. Local hash seeds, arena indices and skiplist
// heights are rebuilt on restore and cannot affect replicated command results.
impl Serialize for SortedSet {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.tree.iter().map(|(score, member)| (member, score.0)))
    }
}

impl<'de> Deserialize<'de> for SortedSet {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SortedSetVisitor;

        impl<'de> Visitor<'de> for SortedSetVisitor {
            type Value = SortedSet;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a sequence of unique sorted-set members and scores")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<SortedSet, A::Error> {
                let size_hint = seq.size_hint().unwrap_or(0);
                // Small sets retain the insertion layout, whose spare block
                // capacity avoids slowing writes after restore. Bincode gives
                // an exact length; formats without a hint use this path too.
                let mut bulk = (size_hint > 4096).then(|| SortedSkipListBuilder::new(size_hint));
                let mut tree = IndexedSkipList::new();
                // Bound initial allocations for untrusted serialized lengths.
                let mut hash = if bulk.is_some() {
                    ScanMap::with_capacity(4096)
                } else {
                    ScanMap::new()
                };
                while let Some((member, score)) = seq.next_element::<(Bytes, f64)>()? {
                    if score.is_nan() {
                        return Err(A::Error::custom("sorted-set score cannot be NaN"));
                    }
                    if hash.insert(member.clone(), score).is_some() {
                        return Err(A::Error::custom("duplicate sorted-set member"));
                    }
                    let key = (OrderedFloat(score), member);
                    let ordered = match &mut bulk {
                        Some(builder) => builder.push(key),
                        None => {
                            if tree.iter().next_back().is_some_and(|last| last >= &key) {
                                false
                            } else {
                                tree.insert(key)
                            }
                        }
                    };
                    if !ordered {
                        return Err(A::Error::custom(
                            "sorted-set entries are not in score/member order",
                        ));
                    }
                }
                Ok(SortedSet {
                    tree: bulk.map_or(tree, SortedSkipListBuilder::finish),
                    hash,
                })
            }
        }

        deserializer.deserialize_seq(SortedSetVisitor)
    }
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
            let old_score = self.hash.get_mut(&member);
            let exists = old_score.is_some();

            // NX: 只添加不存在的 member
            if req.nx && exists {
                continue;
            }

            // XX: 只更新已经存在的 member
            if req.xx && !exists {
                continue;
            }

            if let Some(stored_score) = old_score {
                let old_score = *stored_score;
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
                    self.tree
                        .update_score(&(OrderedFloat(old_score), member.clone()), score);

                    *stored_score = score;

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
        let end_idx = stop_idx as usize + 1;
        let (start, end) = if reverse {
            (self.len() - end_idx, self.len() - start_idx)
        } else {
            (start_idx, end_idx)
        };
        self.collect_rank_range(start, end, reverse)
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
        if matches!(limit, Some((offset, count)) if offset < 0 || count == 0) {
            return Vec::new();
        }
        let (lower, upper) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        if lower.value > upper.value || lower.value.is_nan() || upper.value.is_nan() {
            return Vec::new();
        }
        let below_lower = |(score, _): &(OrderedFloat<f64>, Bytes)| {
            score.0 < lower.value || (lower.exclusive && score.0 == lower.value)
        };
        let within_upper = |(score, _): &(OrderedFloat<f64>, Bytes)| {
            score.0 < upper.value || (!upper.exclusive && score.0 == upper.value)
        };
        let (offset, count) = range_limit(limit);
        // Seek only the starting boundary. nth/nth_back jump LIMIT offsets
        // through spans; the other boundary is checked while producing output.
        if reverse {
            self.tree
                .iter_to_prefix(within_upper)
                .rev()
                .skip(offset)
                .take_while(|entry| !below_lower(entry))
                .take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            self.tree
                .iter_from_prefix(below_lower)
                .skip(offset)
                .take_while(|entry| within_upper(entry))
                .take(count)
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
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
        if matches!(limit, Some((offset, count)) if offset < 0 || count == 0) {
            return Vec::new();
        }
        let (lower, upper) = if reverse {
            (second, first)
        } else {
            (first, second)
        };
        let lower_ok = |member: &Bytes| match lower {
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
        let upper_ok = |member: &Bytes| match upper {
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

        // BYLEX is defined by Redis only when all scores are equal. In that
        // case member comparisons are monotonic in the skiplist, so seek the
        // starting boundary through its upper levels. Keep the deterministic
        // filtering behavior for mixed-score sets, without burdening writes
        // with an additional member index.
        if self.tree.iter().next().map(|entry| entry.0)
            == self.tree.iter().next_back().map(|entry| entry.0)
        {
            let (offset, count) = range_limit(limit);
            return if reverse {
                self.tree
                    .iter_to_prefix(|(_, member)| upper_ok(member))
                    .rev()
                    .skip(offset)
                    .take_while(|(_, member)| lower_ok(member))
                    .take(count)
                    .map(|(score, member)| (member.clone(), score.0))
                    .collect()
            } else {
                self.tree
                    .iter_from_prefix(|(_, member)| !lower_ok(member))
                    .skip(offset)
                    .take_while(|(_, member)| upper_ok(member))
                    .take(count)
                    .map(|(score, member)| (member.clone(), score.0))
                    .collect()
            };
        }
        let in_range = |member: &Bytes| lower_ok(member) && upper_ok(member);

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
        let (start, end) = self.score_ranks(min, max, min_exclusive, max_exclusive);
        end.saturating_sub(start) as i64
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
        if matches!(limit, Some((_, 0))) {
            return Vec::new();
        }
        if min > max || min.is_nan() || max.is_nan() {
            return Vec::new();
        }
        let (offset, count) = limit.unwrap_or((0, usize::MAX));
        self.tree
            .iter_from_prefix(|(score, _)| score.0 < min)
            .skip(offset)
            .take_while(|(score, _)| score.0 <= max)
            .take(count)
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
        let tree = &mut self.tree;
        self.hash.with_removals(|hash| {
            let mut removed = 0i64;
            for member in members {
                if let Some(score) = hash.remove(member) {
                    tree.remove(&(OrderedFloat(score), member.clone()));
                    removed += 1;
                }
            }
            removed
        })
    }

    /// ZINCRBY
    ///
    /// member 不存在时，相当于从 0 开始增加。
    pub fn zincrby(&mut self, member: Bytes, increment: f64) -> Option<f64> {
        let stored_score = self.hash.get_mut(&member);
        let old_score = stored_score.as_deref().copied();
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
            self.tree
                .update_score(&(OrderedFloat(old_score), member.clone()), new_score);
            *stored_score.unwrap() = new_score;
        } else {
            self.tree.insert((OrderedFloat(new_score), member.clone()));
            self.hash.insert(member, new_score);
        }
        Some(new_score)
    }

    /// ZSCORE
    #[inline]
    pub fn zscore(&self, member: &Bytes) -> Option<f64> {
        self.hash.get(member).copied()
    }

    /// ZRANK
    ///
    /// Expected O(log N), using skiplist spans.
    pub fn zrank(&self, member: &Bytes) -> Option<i64> {
        let score = self.hash.get(member).copied()?;
        let key = (OrderedFloat(score), member.clone());
        self.tree.rank(&key).map(|rank| rank as i64)
    }

    /// ZREVRANK
    ///
    /// Expected O(log N), using the forward rank and total length.
    pub fn zrevrank(&self, member: &Bytes) -> Option<i64> {
        self.zrank(member).map(|rank| self.len() as i64 - rank - 1)
    }

    /// 检查集合是否为空。
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// ZPOPMIN
    pub fn zpop_min(&mut self, count: Option<usize>) -> Vec<(Bytes, f64)> {
        let count = match count {
            None => 1,
            Some(0) => return Vec::new(),
            Some(count) => count,
        };
        let tree = &mut self.tree;
        self.hash.with_removals(|hash| {
            let mut values = Vec::with_capacity(count.min(tree.len()));
            for _ in 0..count {
                let (score, member) = match tree.pop_first() {
                    Some(value) => value,
                    None => break,
                };
                hash.remove(&member);
                values.push((member, score.0));
            }
            values
        })
    }

    /// 估算树、哈希表和成员 payload；不包含 SortedSet 的内联大小。
    /// SAMPLES 0 全量遍历，tree/hash 共享的成员字节只计算一次。
    pub fn estimated_heap_usage(&self, samples: usize) -> usize {
        let member_payload = sampled_total(
            self.tree
                .iter()
                .map(|(_, member)| estimated_bytes_heap_usage(member)),
            self.tree.len(),
            samples,
        );

        self.index_allocated_bytes().saturating_add(member_payload)
    }

    /// Storage owned by the indexes, excluding the shared member payloads.
    pub(crate) fn index_allocated_bytes(&self) -> usize {
        self.hash
            .allocated_bytes()
            .saturating_add(self.tree.allocated_bytes())
    }

    /// Stateless bucket scan. COUNT measures visited members before filtering.
    pub fn scan<F>(&self, cursor: u64, count: usize, visit: F) -> u64
    where
        F: FnMut(&Bytes, &f64),
    {
        self.hash.scan(cursor, count, visit)
    }

    /// Unordered members for the canonical replicated ZSCAN path.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&Bytes, &f64)> {
        self.hash.iter()
    }

    /// Rank boundaries for a score interval, without visiting its members.
    fn score_ranks(
        &self,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> (usize, usize) {
        if min > max || min.is_nan() || max.is_nan() {
            return (0, 0);
        }
        let start = self
            .tree
            .partition_point(|(score, _)| score.0 < min || (min_exclusive && score.0 == min));
        let end = self
            .tree
            .partition_point(|(score, _)| score.0 < max || (!max_exclusive && score.0 == max));
        (start, end.max(start))
    }

    /// Seek rank endpoints directly, including reverse traversal.
    fn collect_rank_range(&self, start: usize, end: usize, reverse: bool) -> Vec<(Bytes, f64)> {
        if start >= end {
            return Vec::new();
        }
        let iter = self.tree.iter_range(start, end);
        if reverse {
            iter.rev()
                .map(|(score, member)| (member.clone(), score.0))
                .collect()
        } else {
            iter.map(|(score, member)| (member.clone(), score.0))
                .collect()
        }
    }
}

// Callers have already rejected negative offsets and zero counts.
fn range_limit(limit: Option<(i64, i64)>) -> (usize, usize) {
    match limit {
        None => (0, usize::MAX),
        Some((offset, count)) => (
            offset as usize,
            if count < 0 {
                usize::MAX
            } else {
                count as usize
            },
        ),
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
