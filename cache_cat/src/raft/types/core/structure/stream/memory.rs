//! Approximate reachable allocation accounting, not allocator/RSS profiling.
use std::mem::{align_of, size_of};

pub(crate) const DEFAULT_MEMORY_SAMPLES: usize = 5;

/// Estimated bytes reachable from ONE stream, including reserved capacities.
/// Multiple SharedStream clones refer to the same allocations; do not sum their
/// reports. Shared clocks are charged in full to each logical stream.
///
/// Entry buffers are sampled and extrapolated (five entries plus the tail by
/// default). ART allocations are estimated from entry counts and a fixed fanout
/// model; HashMap/Arc layout is modeled. PEL name copies use consumer name lengths
/// and pending counts. Entry/group/consumer/pending counts remain exact.
/// A zero sample budget scans all entry buffers, but other estimates remain.
/// Allocator metadata, size-class rounding, fragmentation, executor task storage,
/// lock/Notify waiter futures, temporary buffers, and custom clock heap storage
/// not declared via Clock::estimated_heap_bytes are excluded. The report is NOT
/// an upper bound and must not be used as a hard process memory limit.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryUsage {
    pub total_bytes: usize,
    pub stream_inline_bytes: usize,
    pub entry_index_bytes: usize,
    pub entry_buffer_bytes: usize,
    pub group_table_bytes: usize,
    pub consumer_table_bytes: usize,
    pub pel_index_bytes: usize,
    pub name_buffer_bytes: usize,
    pub group_identity_bytes: usize,
    pub clock_bytes: usize,
    pub shared_overhead_bytes: usize,
    pub notification_bytes: usize,
    pub entries: usize,
    pub groups: usize,
    pub consumers: usize,
    /// Authoritative group PEL records; secondary indexes are not counted twice.
    pub pending_entries: usize,
}

impl MemoryUsage {
    pub(crate) fn finish(&mut self) {
        self.total_bytes = [
            self.stream_inline_bytes, self.entry_index_bytes,
            self.entry_buffer_bytes, self.group_table_bytes,
            self.consumer_table_bytes, self.pel_index_bytes,
            self.name_buffer_bytes, self.group_identity_bytes, self.clock_bytes,
            self.shared_overhead_bytes, self.notification_bytes,
        ].into_iter().fold(0usize, usize::saturating_add);
    }
}

fn align_up(n: usize, alignment: usize) -> usize {
    let rem = n % alignment;
    if rem == 0 { n } else { n.saturating_add(alignment - rem) }
}

/// Model ArcInner as strong/weak counters followed by aligned T. Not a stable ABI.
pub(crate) fn arc_allocation_bytes(value_size: usize, value_align: usize) -> usize {
    let header = 2usize.saturating_mul(size_of::<usize>());
    let align = value_align.max(align_of::<usize>());
    align_up(align_up(header, value_align).saturating_add(value_size), align)
}

pub(crate) fn arc_bytes<T>() -> usize {
    arc_allocation_bytes(size_of::<T>(), align_of::<T>())
}

/// SwissTable-style model: ~7/8 maximum load, power-of-two buckets, one control
/// byte per bucket, plus a 16-byte control group. std does not expose allocation
/// size; tombstones and platform/Rust-version differences can alter this estimate.
/// Inline HashMap headers and key/value heap buffers are counted elsewhere.
pub(crate) fn hash_table_bytes<K, V>(capacity: usize) -> usize {
    if capacity == 0 { return 0; }
    let needed = if capacity < 4 { 4 } else if capacity < 8 { 8 } else {
        capacity.saturating_mul(8).saturating_add(6) / 7
    };
    let Some(buckets) = needed.checked_next_power_of_two() else { return usize::MAX; };
    let slots = buckets.saturating_mul(size_of::<(K, V)>());
    align_up(slots, 16).saturating_add(buckets).saturating_add(16)
}
