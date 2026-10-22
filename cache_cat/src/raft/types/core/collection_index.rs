//! Element order used by HSCAN, SSCAN and ZSCAN.
//!
//! Hash fields, set members and sorted-set members live in unordered
//! containers, so every page orders them by a fixed element hash. That keeps a
//! page identical on every replica and keeps a cursor valid across nodes.

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Entries inspected per requested element, so a sparse MATCH pattern cannot
/// turn one call into a full collection walk.
const PAGE_MULTIPLIER: usize = 10;

/// Collections allowed to keep a cached order; the cache is a hint and drops
/// everything once it grows past this.
const MAX_CACHED_COLLECTIONS: usize = 64;

/// Element order of one collection.
#[derive(Debug)]
pub struct CollectionIndex {
    entries: Vec<(u64, Bytes)>,
}

impl CollectionIndex {
    /// Sort by hash, then by element so equal hashes keep one order everywhere.
    pub fn new(elements: impl Iterator<Item = Bytes>) -> Self {
        let mut entries: Vec<(u64, Bytes)> = elements
            .map(|element| (element_hash(&element), element))
            .collect();
        entries.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        });
        Self { entries }
    }

    /// Indexed element count, including elements deleted since the build.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return at most `count` accepted elements after `cursor`, plus the cursor
    /// to resume from, which is zero once the collection is exhausted.
    pub fn page<F>(&self, cursor: u64, count: usize, mut accept: F) -> (u64, Vec<Bytes>)
    where
        F: FnMut(&Bytes) -> bool,
    {
        // The cursor is the hash of the first element the page still owns, and
        // zero starts a new iteration.
        let start = if cursor == 0 {
            0
        } else {
            self.entries.partition_point(|(hash, _)| *hash < cursor)
        };
        let budget = count.saturating_mul(PAGE_MULTIPLIER);
        let mut position = start;
        let mut picked = Vec::new();
        while position < self.entries.len() && picked.len() < count && position - start < budget {
            let group = self.entries[position].0;
            // Always finish the hash group a page reaches: a resumed cursor
            // skips whole groups, so a split group would be lost.
            while position < self.entries.len() && self.entries[position].0 == group {
                if accept(&self.entries[position].1) {
                    picked.push(self.entries[position].1.clone());
                }
                position += 1;
            }
        }
        let next = self.entries.get(position).map_or(0, |(hash, _)| *hash);
        (next, picked)
    }
}

/// Element orders of one database, dropped whenever the journal is wiped.
#[derive(Debug, Default)]
pub struct CollectionIndexCache {
    indexes: Mutex<HashMap<Bytes, Arc<CollectionIndex>>>,
}

impl CollectionIndexCache {
    /// Cached order of one collection, rebuilt when missing, forced, or left
    /// much larger than the live elements by deletions.
    pub fn get(
        &self,
        key: &Bytes,
        live_len: usize,
        rebuild: bool,
        build: impl FnOnce() -> Vec<Bytes>,
    ) -> Arc<CollectionIndex> {
        let oversized =
            |index: &CollectionIndex| index.len() > live_len.saturating_mul(2).saturating_add(1024);

        if !rebuild
            && let Some(index) = self.indexes.lock().get(key).cloned()
            && !oversized(&index)
        {
            return index;
        }

        let index = Arc::new(CollectionIndex::new(build().into_iter()));
        let mut indexes = self.indexes.lock();
        if indexes.len() >= MAX_CACHED_COLLECTIONS {
            indexes.clear();
        }
        indexes.insert(key.clone(), Arc::clone(&index));
        index
    }

    /// Drop every cached order.
    pub fn clear(&self) {
        self.indexes.lock().clear();
    }
}

/// Fixed hash of an element, independent of insertion order and node state.
fn element_hash(element: &Bytes) -> u64 {
    let mut hasher = DefaultHasher::new();
    element.hash(&mut hasher);
    hasher.finish()
}
