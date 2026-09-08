use super::core::{Consumer, Group, RedisStream};
use crate::raft::types::core::structure::stream::memory::{
    arc_allocation_bytes, arc_bytes, hash_table_bytes, MemoryUsage,
};
use blart::{visitor::TreeStatsCollector, TreeMap};
use std::mem::{align_of_val, size_of, size_of_val};

fn tree_bytes<V>(tree: &TreeMap<[u8; 16], V>) -> usize {
    TreeStatsCollector::collect(tree).map_or(0, |s| s.total_memory_usage())
}

impl RedisStream {
    /// O(ART nodes + field pairs + group/consumer HashMap capacities + PEL
    /// records), excluding byte-buffer contents: only their capacities are read.
    /// Use for occasional monitoring, not on every append/ACK.
    pub fn memory_usage(&self) -> MemoryUsage {
        let mut usage = MemoryUsage {
            stream_inline_bytes: size_of::<Self>(),
            entry_index_bytes: tree_bytes(&self.entries),
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
        for fields in self.entries.values() {
            usage.entry_buffer_bytes = usage.entry_buffer_bytes.saturating_add(
                fields
                    .capacity()
                    .saturating_mul(size_of::<(Vec<u8>, Vec<u8>)>()),
            );
            for (name, value) in fields {
                usage.entry_buffer_bytes = usage
                    .entry_buffer_bytes
                    .saturating_add(name.capacity())
                    .saturating_add(value.capacity());
            }
        }
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
            for pending in group.pel.values() {
                usage.name_buffer_bytes = usage
                    .name_buffer_bytes
                    .saturating_add(pending.consumer.capacity());
            }
            for (name, consumer) in &group.consumers {
                usage.name_buffer_bytes = usage.name_buffer_bytes.saturating_add(name.capacity());
                usage.pel_index_bytes = usage
                    .pel_index_bytes
                    .saturating_add(tree_bytes(&consumer.pel));
            }
        }
        usage.finish();
        usage
    }

    pub fn estimated_memory_usage(&self) -> usize {
        self.memory_usage().total_bytes
    }
}
