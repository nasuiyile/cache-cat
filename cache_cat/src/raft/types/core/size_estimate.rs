use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard};
use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use crate::raft::types::core::value_object::HashValue;
use crate::raft::types::core::sorted_set::SortedSet;

#[inline]
pub fn estimated_bytes_heap_usage(value: &Bytes) -> usize {
    value.len()
}

/// ---------------------------------------------------------------------------
/// Mutex
/// ---------------------------------------------------------------------------

/// MEMORY USAGE 属于诊断型读操作。
///
/// 即使 Mutex 曾经被 poison，也继续读取当前数据，而不是再次 panic。
#[inline]
fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock()
}

/// ---------------------------------------------------------------------------
/// Arc
/// ---------------------------------------------------------------------------

/// Arc<T> 的 heap allocation 大致包含：
///
///     strong count
///     weak count
///     T
///
/// Arc 指针本身已经包含在 ValueObject inline size 中，
/// 所以这里只统计 Arc 在 heap 上的 control block + T。
///
/// 实际 allocator 可能因为 alignment 增加额外 padding，
/// MEMORY USAGE 不追求 allocator 级别完全精确。
#[inline]
fn estimate_arc_allocation<T>() -> usize {
    size_of::<usize>()
        .saturating_mul(2)
        .saturating_add(size_of::<T>())
}

/// ---------------------------------------------------------------------------
/// Sampling
/// ---------------------------------------------------------------------------

/// 根据 Redis MEMORY USAGE SAMPLES 的语义计算 collection payload。
///
/// samples:
///
///     0 => 遍历所有元素
///     N => 最多检查 N 个元素，然后按平均值推算总大小
///
/// 例如：
///
///     MEMORY USAGE key
///
/// 默认调用方传 5。
///
///     MEMORY USAGE key SAMPLES 0
///
/// 则这里遍历整个集合。
fn sampled_total<I>(
    values: I,
    total_elements: usize,
    samples: usize,
) -> usize
where
    I: Iterator<Item = usize>,
{
    if total_elements == 0 {
        return 0;
    }

    let target_samples = if samples == 0 {
        total_elements
    } else {
        samples.min(total_elements)
    };

    if target_samples == 0 {
        return 0;
    }

    let mut sampled_bytes = 0usize;
    let mut actual_samples = 0usize;

    for size in values.take(target_samples) {
        sampled_bytes = sampled_bytes.saturating_add(size);
        actual_samples += 1;
    }

    if actual_samples == 0 {
        return 0;
    }

    /*
     * 如果已经扫描全部元素，不需要做 estimate。
     */
    if actual_samples >= total_elements {
        return sampled_bytes;
    }

    /*
     * estimated =
     *
     *     sampled_bytes / sample_count * total_count
     *
     * 使用 u128 避免 usize 乘法溢出。
     */
    let estimated = (sampled_bytes as u128)
        .saturating_mul(total_elements as u128)
        / actual_samples as u128;

    estimated.min(usize::MAX as u128) as usize
}


pub fn estimate_list_usage(
    value: &Arc<Mutex<VecDeque<Bytes>>>,
    samples: usize,
) -> usize {
    let list = lock_unpoisoned(value);

    /*
     * Arc heap：
     *
     * ArcInner {
     *     strong,
     *     weak,
     *     Mutex<VecDeque<Bytes>>,
     * }
     *
     * VecDeque 本身 metadata 已经包含在这里。
     */
    let arc_allocation =
        estimate_arc_allocation::<Mutex<VecDeque<Bytes>>>();

    /*
     * VecDeque 的 heap buffer。
     *
     * 必须用 capacity，而不是 len。
     *
     * 例如：
     *
     *     len      = 10
     *     capacity = 16
     *
     * 那么实际上已经为 16 个 Bytes slot 分配了空间。
     */
    let deque_buffer = list
        .capacity()
        .saturating_mul(size_of::<Bytes>());

    /*
     * 每个 Bytes 真正指向的 payload。
     */
    let payload = sampled_total(
        list.iter()
            .map(estimated_bytes_heap_usage),
        list.len(),
        samples,
    );

    arc_allocation
        .saturating_add(deque_buffer)
        .saturating_add(payload)
}

/// ---------------------------------------------------------------------------
/// SET
/// ---------------------------------------------------------------------------

pub fn estimate_set_usage(
    value: &Arc<Mutex<HashSet<Bytes>>>,
    samples: usize,
) -> usize {
    let set = lock_unpoisoned(value);

    let arc_allocation =
        estimate_arc_allocation::<Mutex<HashSet<Bytes>>>();

    /*
     * std::collections::HashSet 的内部 raw-table layout
     * 不属于 Rust stable API，所以不能精确查询实际 bucket bytes。
     *
     * 这里估算为：
     *
     *     capacity * (
     *         size_of::<Bytes>()
     *         + control byte
     *     )
     *
     * 1 byte control metadata 是近似值。
     */
    let bucket_size =
        size_of::<Bytes>()
            .saturating_add(1);

    let hash_table = set
        .capacity()
        .saturating_mul(bucket_size);

    let payload = sampled_total(
        set.iter()
            .map(estimated_bytes_heap_usage),
        set.len(),
        samples,
    );

    arc_allocation
        .saturating_add(hash_table)
        .saturating_add(payload)
}



pub fn estimate_hash_usage(
    value: &Arc<Mutex<HashMap<Bytes, HashValue>>>,
    samples: usize,
) -> usize {
    let hash = lock_unpoisoned(value);

    let arc_allocation =
        estimate_arc_allocation::<Mutex<HashMap<Bytes, HashValue>>>();

    /*
     * HashMap bucket 保存：
     *
     *     Bytes
     *     HashValue
     *
     * 使用 tuple size 可以自然考虑 struct alignment / padding。
     *
     * 最后 +1 是 approximate control byte。
     */
    let bucket_size =
        size_of::<(Bytes, HashValue)>()
            .saturating_add(1);

    let hash_table = hash
        .capacity()
        .saturating_mul(bucket_size);

    /*
     * field Bytes payload
     *
     * +
     *
     * HashValue 自己的动态 heap payload。
     */
    let payload = sampled_total(
        hash.iter().map(|(field, value)| {
            estimated_bytes_heap_usage(field)
                .saturating_add(
                    estimated_hash_value_heap_usage(value)
                )
        }),
        hash.len(),
        samples,
    );

    arc_allocation
        .saturating_add(hash_table)
        .saturating_add(payload)
}


#[inline]
fn estimated_hash_value_heap_usage(
    _value: &HashValue,
) -> usize {
    0
}

/// ---------------------------------------------------------------------------
/// ZSET
/// ---------------------------------------------------------------------------

pub fn estimate_zset_usage(
    value: &Arc<Mutex<SortedSet>>,
    samples: usize,
) -> usize {
    let zset = lock_unpoisoned(value);

    /*
     * 这里统计：
     *
     * ArcInner {
     *     strong,
     *     weak,
     *     Mutex<SortedSet>
     * }
     *
     * Mutex<SortedSet> 内已经包含：
     *
     *     BTreeSet metadata
     *     HashMap metadata
     *
     * 所以 SortedSet::estimated_heap_usage()
     * 只应该统计 tree/hash 真正申请的 heap storage。
     */
    let arc_allocation =
        estimate_arc_allocation::<Mutex<SortedSet>>();

    arc_allocation
        .saturating_add(
            zset.estimated_heap_usage(samples)
        )
}

pub fn estimate_bloom_usage(
    value: &Arc<Mutex<BloomObject>>,
) -> usize {
    let bloom = value.lock();

    /*
     * ValueObject 中 Arc handle 自己属于 enum inline storage，
     * 已经由 size_of::<ValueObject>() 统计。
     *
     * 这里统计 Arc 指向的 allocation。
     *
     * 一个 Arc allocation 逻辑上包括：
     *
     * strong count
     * weak count
     * Mutex<BloomObject>
     */
    let arc_counters = size_of::<AtomicUsize>()
        .saturating_mul(2);

    arc_counters
        .saturating_add(
            size_of::<Mutex<BloomObject>>()
        )
        .saturating_add(
            bloom.estimated_heap_usage()
        )
}