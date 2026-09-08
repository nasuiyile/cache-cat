//! 估算单个值的堆内存；ValueObject 的内联大小由调用方统计。
//! 容器布局和分配器开销是近似值，共享分配不做跨键去重。

use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use crate::raft::types::core::structure::sorted_set::SortedSet;
use crate::raft::types::core::value_object::HashValue;
use bytes::Bytes;
use parking_lot::Mutex;
use std::alloc::Layout;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// Bytes 不公开底层容量，按可见长度计费；切片、静态数据及共享缓冲区会有偏差。
#[inline]
pub fn estimated_bytes_heap_usage(value: &Bytes) -> usize {
    value.len()
}

/// Arc 指针已包含在值中，此处统计引用计数、T 和对齐填充。
fn estimate_arc_allocation<T>() -> usize {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map_or(usize::MAX, |(layout, _)| layout.pad_to_align().size())
}

/// 按 SwissTable 的装载率推算桶数，加上控制字节和 16 字节控制组。
/// std 不公开实际布局；删除产生的墓碑可能使 capacity 低于分配的可用槽数。
pub(crate) fn estimate_hash_table_usage<T>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 4 {
        4
    } else if capacity < 8 {
        8
    } else {
        let Some(buckets) = capacity
            .checked_mul(8)
            .and_then(|n| n.checked_add(6))
            .and_then(|n| (n / 7).checked_next_power_of_two())
        else {
            return usize::MAX;
        };
        buckets
    };
    let data = buckets.saturating_mul(size_of::<T>());
    let padding = (16 - data % 16) % 16;
    data.saturating_add(padding)
        .saturating_add(buckets)
        .saturating_add(16)
}

/// SAMPLES 0 遍历全部元素；其他值按采样平均值推算总 payload。
pub(crate) fn sampled_total<I>(values: I, total_elements: usize, samples: usize) -> usize
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
    let mut sampled_bytes = 0usize;
    let mut actual_samples = 0usize;
    for size in values.take(target_samples) {
        sampled_bytes = sampled_bytes.saturating_add(size);
        actual_samples += 1;
    }
    if actual_samples == 0 {
        return 0;
    }
    if actual_samples == total_elements {
        return sampled_bytes;
    }

    let estimated = (sampled_bytes as u128) * (total_elements as u128) / actual_samples as u128;
    estimated.min(usize::MAX as u128) as usize
}

pub fn estimate_list_usage(value: &Arc<Mutex<VecDeque<Bytes>>>, samples: usize) -> usize {
    let list = value.lock();
    let deque_buffer = list.capacity().saturating_mul(size_of::<Bytes>());
    let payload = sampled_total(
        list.iter().map(estimated_bytes_heap_usage),
        list.len(),
        samples,
    );
    estimate_arc_allocation::<Mutex<VecDeque<Bytes>>>()
        .saturating_add(deque_buffer)
        .saturating_add(payload)
}

pub fn estimate_set_usage(value: &Arc<Mutex<HashSet<Bytes>>>, samples: usize) -> usize {
    let set = value.lock();
    let hash_table = estimate_hash_table_usage::<Bytes>(set.capacity());
    let payload = sampled_total(
        set.iter().map(estimated_bytes_heap_usage),
        set.len(),
        samples,
    );
    estimate_arc_allocation::<Mutex<HashSet<Bytes>>>()
        .saturating_add(hash_table)
        .saturating_add(payload)
}

pub fn estimate_hash_usage(value: &Arc<Mutex<HashMap<Bytes, HashValue>>>, samples: usize) -> usize {
    let hash = value.lock();
    let hash_table = estimate_hash_table_usage::<(Bytes, HashValue)>(hash.capacity());
    let payload = sampled_total(
        hash.iter().map(|(field, value)| {
            estimated_bytes_heap_usage(field).saturating_add(match value {
                HashValue::Str(bytes) => estimated_bytes_heap_usage(bytes),
                HashValue::Int(_) => 0,
            })
        }),
        hash.len(),
        samples,
    );
    estimate_arc_allocation::<Mutex<HashMap<Bytes, HashValue>>>()
        .saturating_add(hash_table)
        .saturating_add(payload)
}

pub fn estimate_zset_usage(value: &Arc<Mutex<SortedSet>>, samples: usize) -> usize {
    let zset = value.lock();
    estimate_arc_allocation::<Mutex<SortedSet>>().saturating_add(zset.estimated_heap_usage(samples))
}

pub fn estimate_bloom_usage(value: &Arc<Mutex<BloomObject>>) -> usize {
    let bloom = value.lock();
    estimate_arc_allocation::<Mutex<BloomObject>>().saturating_add(bloom.estimated_heap_usage())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::structure::stream::RedisStream;
    use crate::raft::types::core::value_object::ValueObject;

    #[test]
    fn hash_string_payload_is_counted_in_sampled_and_full_scans() {
        let hash = Arc::new(Mutex::new(HashMap::from([
            (Bytes::from_static(b"a"), HashValue::Int(1)),
            (Bytes::from_static(b"b"), HashValue::Int(2)),
        ])));
        let before = estimate_hash_usage(&hash, 0);
        for value in hash.lock().values_mut() {
            *value = HashValue::Str(Bytes::from(vec![b'x'; 128]));
        }
        for samples in [0, 1, 5] {
            assert_eq!(estimate_hash_usage(&hash, samples), before + 256);
        }
    }

    #[test]
    fn reserved_set_counts_unused_buckets_and_control_group() {
        let set = Arc::new(Mutex::new(HashSet::<Bytes>::with_capacity(3)));
        assert_eq!(set.lock().capacity(), 3);
        let buckets = 4 * size_of::<Bytes>();
        let controls = 4 + 16;
        assert_eq!(
            estimate_set_usage(&set, 0),
            estimate_arc_allocation::<Mutex<HashSet<Bytes>>>() + buckets + controls
        );
    }

    #[test]
    fn reserved_list_keeps_its_buffer_cost_after_removal() {
        let list = Arc::new(Mutex::new(VecDeque::with_capacity(16)));
        let empty = estimate_list_usage(&list, 0);
        list.lock().push_back(Bytes::from(vec![b'x'; 128]));
        assert_eq!(estimate_list_usage(&list, 0), empty + 128);
        list.lock().pop_front();
        assert_eq!(estimate_list_usage(&list, 0), empty);
    }

    #[test]
    fn zset_shared_member_payload_is_counted_once() {
        let short = Arc::new(Mutex::new(SortedSet::new()));
        let long = Arc::new(Mutex::new(SortedSet::new()));
        short
            .lock()
            .zincrby(Bytes::from(vec![b'x'; 1]), 1.0)
            .unwrap();
        long.lock()
            .zincrby(Bytes::from(vec![b'x'; 129]), 1.0)
            .unwrap();
        for samples in [0, 1, 5] {
            assert_eq!(
                estimate_zset_usage(&long, samples) - estimate_zset_usage(&short, samples),
                128
            );
        }
    }

    #[test]
    fn allocation_estimates_handle_alignment_and_overflow() {
        #[repr(align(64))]
        struct Aligned;

        assert_eq!(estimate_arc_allocation::<Aligned>(), 64);
        assert_eq!(estimate_hash_table_usage::<Bytes>(0), 0);
        assert_eq!(estimate_hash_table_usage::<Bytes>(usize::MAX), usize::MAX);
        assert!(estimate_hash_table_usage::<Bytes>(14) > estimate_hash_table_usage::<Bytes>(7));
    }

    #[test]
    fn stream_inline_storage_is_only_counted_once() {
        let stream = RedisStream::new();
        let usage = stream.memory_usage();
        let value = ValueObject::Stream(stream);
        let heap = usage.total_bytes - usage.stream_inline_bytes;
        assert_eq!(value.estimated_heap_usage(0), heap);
        assert_eq!(
            value.estimated_memory_usage(0),
            size_of::<ValueObject>() + heap
        );
    }

    #[test]
    fn sampling_supports_full_scans_limits_and_saturation() {
        assert_eq!(sampled_total([1, 2, 9].into_iter(), 3, 0), 12);
        assert_eq!(sampled_total([1, 2, 9].into_iter(), 3, 2), 4);
        assert_eq!(sampled_total([1, 2, 9].into_iter(), 3, 5), 12);
        assert_eq!(sampled_total(std::iter::empty(), 0, 0), 0);
        assert_eq!(sampled_total([usize::MAX].into_iter(), 2, 1), usize::MAX);
        assert_eq!(sampled_total([usize::MAX, 1].into_iter(), 2, 0), usize::MAX);
    }
}
