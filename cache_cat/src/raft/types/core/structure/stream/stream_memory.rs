use super::core::{Consumer, Group, Key, RedisStream};
use super::types::Fields;
use crate::raft::types::core::structure::stream::memory::{
    DEFAULT_MEMORY_SAMPLES, MemoryUsage, arc_allocation_bytes, arc_bytes, hash_table_bytes,
};
use blart::{
    TreeMap,
    raw::{InnerNodeSorted, LeafNode},
};
use std::mem::{align_of_val, size_of, size_of_val};

/// Like Redis's streamRadixTreeMemoryUsage, model structural overhead without
/// visiting nodes. blart has one leaf per entry; approximate inner nodes as a
/// four-way tree (ceil((leaves - 1) / 3) Node4 allocations). Actual ART fanout
/// and prefix compression can differ. TreeMap's inline header is counted by
/// its containing stream/group/consumer, and V is included in the leaf.
fn tree_bytes<V, const PREFIX_LEN: usize>(tree: &TreeMap<Key, V, PREFIX_LEN>) -> usize {
    let leaves = tree.len();
    let inner_nodes = leaves.saturating_sub(1).div_ceil(3);
    leaves
        .saturating_mul(size_of::<LeafNode<Key, V, PREFIX_LEN>>())
        .saturating_add(
            inner_nodes.saturating_mul(size_of::<InnerNodeSorted<Key, V, PREFIX_LEN, 4>>()),
        )
}

fn field_buffer_bytes(fields: &Fields) -> usize {
    fields.iter().fold(
        fields
            .capacity()
            .saturating_mul(size_of::<(Vec<u8>, Vec<u8>)>()),
        |bytes, (name, value)| {
            bytes
                .saturating_add(name.capacity())
                .saturating_add(value.capacity())
        },
    )
}

/// Redis 8.2 object.c/objectComputeSize samples the first N listpacks, then
/// extrapolates all but the last and adds the last allocation separately.
/// This stream stores Fields per leaf instead of listpacks, so sample those
/// buffers. Zero means all entries, as with Redis MEMORY USAGE SAMPLES 0.
fn entry_buffer_bytes(entries: &TreeMap<Key, Fields>, samples: usize) -> usize {
    let count = if samples == 0 {
        entries.len()
    } else {
        samples.min(entries.len())
    };
    let sampled_bytes = entries
        .values()
        .take(count)
        .map(field_buffer_bytes)
        .fold(0usize, usize::saturating_add);
    if count == entries.len() {
        return sampled_bytes;
    }

    // Redis's Stream path truncates the average BEFORE extrapolating; the
    // generic collection estimator instead retains the fractional average.
    let estimate = (sampled_bytes / count).saturating_mul(entries.len() - 1);
    estimate.saturating_add(field_buffer_bytes(
        entries.last_key_value().expect("nonempty sampled stream").1,
    ))
}

impl RedisStream {
    /// Estimate memory using the first five entries and the last entry.
    /// Indexes/PELs are modeled from their lengths, without walking their nodes.
    /// See memory_usage_with_samples for accuracy and complexity tradeoffs.
    pub fn memory_usage(&self) -> MemoryUsage {
        self.memory_usage_with_samples(DEFAULT_MEMORY_SAMPLES)
    }

    /// Redis-style sampling: extrapolate the first `samples` entries and count
    /// the tail separately. Zero scans all entry buffers; indexes remain modeled.
    /// Cost is O(field pairs in sampled entries and tail + group/consumer HashMap
    /// capacities), independent of total entry/PEL length with a fixed sample
    /// budget. Skewed payloads outside the sample can cause substantial error.
    pub fn memory_usage_with_samples(&self, samples: usize) -> MemoryUsage {
        let mut usage = MemoryUsage {
            stream_inline_bytes: size_of::<Self>(),
            entry_index_bytes: tree_bytes(&self.entries),
            entry_buffer_bytes: entry_buffer_bytes(&self.entries, samples),
            group_table_bytes: hash_table_bytes::<Vec<u8>, Group>(self.groups.capacity()),
            clock_bytes: arc_allocation_bytes(
                size_of_val(self.clock.as_ref()),
                align_of_val(self.clock.as_ref()),
            )
            .saturating_add(self.clock.estimated_heap_bytes()),
            entries: self.entries.len(),
            groups: self.groups.len(),
            ..MemoryUsage::default()
        };
        for (name, group) in &self.groups {
            usage.name_buffer_bytes = usage.name_buffer_bytes.saturating_add(name.capacity());
            usage.group_identity_bytes =
                usage.group_identity_bytes.saturating_add(arc_bytes::<()>());
            usage.consumer_table_bytes =
                usage
                    .consumer_table_bytes
                    .saturating_add(hash_table_bytes::<Vec<u8>, Consumer>(
                        group.consumers.capacity(),
                    ));
            usage.pel_index_bytes = usage.pel_index_bytes.saturating_add(tree_bytes(&group.pel));
            usage.pending_entries = usage.pending_entries.saturating_add(group.pel.len());
            usage.consumers = usage.consumers.saturating_add(group.consumers.len());
            for (name, consumer) in &group.consumers {
                // Each group PEL record owns a copy of its consumer name.
                // Estimate those copies from the secondary index's length;
                // Pending itself was already counted in the group PEL leaves.
                usage.name_buffer_bytes = usage
                    .name_buffer_bytes
                    .saturating_add(name.capacity())
                    .saturating_add(name.len().saturating_mul(consumer.pel.len()));
                usage.pel_index_bytes = usage
                    .pel_index_bytes
                    .saturating_add(tree_bytes(&consumer.pel));
            }
        }
        usage.finish();
        usage
    }

    /// Convenience total using the default five-entry sample budget.
    pub fn estimated_memory_usage(&self) -> usize {
        self.memory_usage().total_bytes
    }
}


