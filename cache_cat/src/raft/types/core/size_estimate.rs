//! 估算单个值的堆内存；ValueObject 的内联大小由调用方统计。
//! 容器布局和分配器开销是近似值，共享分配不做跨键去重。
//! 采样策略参照 Redis 8.2 kvobjComputeSize：按迭代顺序取前 N 项，
//! 平均值外推；本项目没有 listpack/intset/quicklist 编码，不套用其阈值。

use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use crate::raft::types::core::structure::sorted_set::SortedSet;
use crate::raft::types::core::structure::scan_map::ScanMap as HashMap;
use crate::raft::types::core::structure::scan_set::ScanSet;
use crate::raft::types::core::value_object::HashValue;
use bytes::Bytes;
use parking_lot::Mutex;
use std::alloc::Layout;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

/// Bytes 不公开底层容量，按可见长度计费；切片、静态数据及共享缓冲区会有偏差。
#[inline]
pub fn estimated_bytes_heap_usage(value: &Bytes) -> usize {
    value.len()
}

/// Arc 指针已包含在值中，此处统计引用计数、T 和对齐填充。
pub(crate) fn estimate_arc_allocation<T>() -> usize {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map_or(usize::MAX, |(layout, _)| layout.pad_to_align().size())
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

pub fn estimate_set_usage(value: &Arc<Mutex<ScanSet<Bytes>>>, samples: usize) -> usize {
    let set = value.lock();
    let hash_table = set.allocated_bytes();
    let payload = sampled_total(
        set.iter().map(estimated_bytes_heap_usage),
        set.len(),
        samples,
    );
    estimate_arc_allocation::<Mutex<ScanSet<Bytes>>>()
        .saturating_add(hash_table)
        .saturating_add(payload)
}

pub fn estimate_hash_usage(value: &Arc<Mutex<HashMap<Bytes, HashValue>>>, samples: usize) -> usize {
    let hash = value.lock();
    let hash_table = hash.allocated_bytes();
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
    use crate::raft::types::core::structure::stream::SharedStream;
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
    fn reserved_set_counts_backing_arrays_and_retains_them_after_removal() {
        let set = Arc::new(Mutex::new(ScanSet::<Bytes>::with_capacity(3)));
        let reserved = estimate_set_usage(&set, 0);
        assert_eq!(
            reserved,
            estimate_arc_allocation::<Mutex<ScanSet<Bytes>>>() + set.lock().allocated_bytes()
        );
        assert!(reserved > estimate_arc_allocation::<Mutex<ScanSet<Bytes>>>());

        let member = Bytes::from(vec![b'x'; 128]);
        set.lock().insert(member.clone());
        for samples in [0, 1, 5] {
            assert_eq!(estimate_set_usage(&set, samples), reserved + 128);
        }
        assert!(set.lock().remove(&member));
        assert_eq!(estimate_set_usage(&set, 0), reserved);
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
        // Independent skiplists may choose different random node heights.
        // Subtract their actual index allocations to isolate member payloads.
        let short_index = short.lock().index_allocated_bytes();
        let long_index = long.lock().index_allocated_bytes();
        for samples in [0, 1, 5] {
            assert_eq!(
                (estimate_zset_usage(&long, samples) - long_index)
                    - (estimate_zset_usage(&short, samples) - short_index),
                128
            );
        }
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

    #[test]
    fn sampling_only_visits_the_requested_number_of_elements() {
        for (samples, expected_visits) in [(0, 10), (1, 1), (5, 5), (100, 10)] {
            let mut visits = 0;
            let values = (0..10).map(|_| {
                visits += 1;
                17
            });
            assert_eq!(sampled_total(values, 10, samples), 170);
            assert_eq!(visits, expected_visits);
        }
    }
}
