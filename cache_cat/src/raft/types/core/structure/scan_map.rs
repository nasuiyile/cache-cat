//! A compact chained hash table with stateless, resize-safe bucket scans.
//!
//! Entries live in one dense allocation. Bucket chains contain indices rather
//! than separately allocated nodes, so ordinary iteration is O(len), including
//! after many deletions. Growth rebuilds all chains synchronously; deletion does
//! not shrink the bucket array. There is no per-client iterator or scan state.

use ahash::RandomState;
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::ops::Index;

const EMPTY: usize = usize::MAX;
const MIN_BUCKETS: usize = 1;

#[derive(Clone, Copy)]
struct Bucket {
    head: usize,
    // Reject most misses without a dependent load from the larger entries
    // allocation. Bits come from the opposite end of the bucket-index bits.
    fingerprints: u64,
}

const EMPTY_BUCKET: Bucket = Bucket {
    head: EMPTY,
    fingerprints: 0,
};

#[inline]
fn fingerprint(hash: u64) -> u64 {
    1 << (hash >> 58)
}

#[derive(Clone)]
struct Entry<K, V> {
    hash: u64,
    next: usize,
    key: K,
    value: V,
}

/// HashMap-compatible operations used by Redis Hash values, plus bucket scans.
///
/// Hashes are cached, so resizing and fixing a moved entry after removal do not
/// rehash field bytes. The default hasher uses randomized keys to resist
/// precomputed collision sets. Ordinary iteration order is unspecified.
/// Hash/ZSET use the default 0.5 bucket load; Set opts into 0.75 at compile time
/// (the one- and two-bucket cases permit full occupancy).
#[derive(Clone)]
pub struct ScanMap<K, V, S = RandomState, const DENSE_BUCKETS: bool = false> {
    entries: Vec<Entry<K, V>>,
    buckets: Vec<Bucket>,
    hash_builder: S,
}

impl<K, V> ScanMap<K, V> {
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, V, S, const DENSE_BUCKETS: bool> ScanMap<K, V, S, DENSE_BUCKETS> {
    pub fn with_hasher(hash_builder: S) -> Self {
        Self {
            entries: Vec::new(),
            buckets: Vec::new(),
            hash_builder,
        }
    }

    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        // The caller already knows the expected field count. Only buckets need
        // a power-of-two size; rounding dense entries here would waste memory
        // for restored hashes and explicitly reserved, non-power-of-two sizes.
        let mut map = Self {
            entries: Vec::with_capacity(capacity),
            buckets: Vec::new(),
            hash_builder,
        };
        map.reserve(capacity);
        map
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries that fit without reallocating either backing array.
    pub fn capacity(&self) -> usize {
        Self::bucket_capacity(self.buckets.len()).min(self.entries.capacity())
    }

    #[inline]
    fn bucket_capacity(buckets: usize) -> usize {
        if DENSE_BUCKETS {
            buckets - buckets / 4
        } else {
            buckets / 2
        }
    }

    pub fn hasher(&self) -> &S {
        &self.hash_builder
    }

    /// Heap storage for the table itself; excludes allocations owned by K/V.
    pub fn allocated_bytes(&self) -> usize {
        self.entries
            .capacity()
            .saturating_mul(size_of::<Entry<K, V>>())
            .saturating_add(self.buckets.capacity().saturating_mul(size_of::<Bucket>()))
    }

    pub fn reserve(&mut self, additional: usize) {
        let needed = self
            .len()
            .checked_add(additional)
            .expect("hash table capacity overflow");
        if needed > Self::bucket_capacity(self.buckets.len()) {
            let capacity = needed
                .max(MIN_BUCKETS)
                .checked_next_power_of_two()
                .and_then(|capacity| {
                    if !DENSE_BUCKETS || needed > Self::bucket_capacity(capacity) {
                        capacity.checked_mul(2)
                    } else {
                        Some(capacity)
                    }
                })
                .expect("hash table capacity overflow");
            self.rehash(capacity);
        }
        if needed > self.entries.capacity() {
            let capacity = needed
                .max(MIN_BUCKETS)
                .checked_next_power_of_two()
                .expect("hash table capacity overflow");
            self.entries.reserve_exact(capacity - self.len());
        }
    }

    /// Clone for Set snapshot writes without discarding useful entry capacity.
    /// Very sparse bucket arrays are rebuilt with the same hashes and seed.
    /// Shrinking can repeat scan results, but cannot skip a stable member.
    pub(super) fn clone_for_write(&self) -> Self
    where
        K: Clone,
        V: Clone,
        S: Clone,
    {
        let len = self.len();
        let capacity = if len == 0 {
            0
        } else {
            let minimum = len.checked_add(1).unwrap_or(len);
            let ceiling = minimum
                .checked_next_power_of_two()
                .unwrap_or(self.entries.capacity());
            // Keep ordinary growth slack, bound historical excess after many
            // removals, and leave at least one slot for the following SADD.
            minimum.max(self.entries.capacity().min(ceiling))
        };
        let mut entries = Vec::with_capacity(capacity);
        entries.extend_from_slice(&self.entries);
        let shrink = self.buckets.len() > len.saturating_mul(8);
        let mut cloned = Self {
            entries,
            buckets: if shrink {
                Vec::new()
            } else {
                self.buckets.clone()
            },
            hash_builder: self.hash_builder.clone(),
        };
        if shrink {
            cloned.reserve(0);
        }
        cloned
    }

    /// Removes all entries while keeping the allocation and hash seed.
    pub fn clear(&mut self) {
        self.buckets.fill(EMPTY_BUCKET);
        self.entries.clear();
    }

    /// Retains matching entries without reallocating or hashing keys again.
    /// Dense maps compact entries and rebuild links once after removals;
    /// sparse maps repair only affected chains. Both paths remain valid if
    /// the predicate or a removed entry's destructor unwinds.
    pub fn retain<F>(&mut self, keep: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.retain_inner::<false, F>(keep);
    }

    /// Retain with the same swap-removal order for every hash seed and capacity.
    /// Sets persist this dense order so replicated index sampling is stable.
    pub(super) fn retain_swap_remove<F>(&mut self, keep: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.retain_inner::<true, F>(keep);
    }

    fn retain_inner<const SWAP_REMOVE: bool, F>(&mut self, mut keep: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        // After heavy deletion, clearing every bucket would dominate the
        // remaining members. Repair just the affected chains in sparse maps.
        if self.buckets.len() > self.entries.len().saturating_mul(8) {
            self.retain_by_removal(keep);
            return;
        }

        struct RebuildOnDrop<'a, K, V, S, const DENSE_BUCKETS: bool> {
            map: &'a mut ScanMap<K, V, S, DENSE_BUCKETS>,
            changed: bool,
        }

        impl<K, V, S, const DENSE_BUCKETS: bool> Drop for RebuildOnDrop<'_, K, V, S, DENSE_BUCKETS> {
            fn drop(&mut self) {
                if !self.changed {
                    return;
                }
                // Both vector removal paths leave dense entries valid before
                // dropping members. Rebuild their index even during unwinding.
                self.map.buckets.fill(EMPTY_BUCKET);
                let mask = self.map.buckets.len() - 1;
                for (index, entry) in self.map.entries.iter_mut().enumerate() {
                    let bucket = &mut self.map.buckets[entry.hash as usize & mask];
                    entry.next = bucket.head;
                    bucket.head = index;
                    bucket.fingerprints |= fingerprint(entry.hash);
                }
            }
        }

        let mut guard = RebuildOnDrop {
            map: self,
            changed: false,
        };
        let RebuildOnDrop { map, changed } = &mut guard;
        if SWAP_REMOVE {
            let mut index = 0;
            while index < map.entries.len() {
                let entry = &mut map.entries[index];
                if keep(&entry.key, &mut entry.value) {
                    index += 1;
                } else {
                    // Match the sparse path: inspect the moved tail next.
                    // Mark before dropping the removed key/value can panic.
                    *changed = true;
                    map.entries.swap_remove(index);
                }
            }
        } else {
            map.entries.retain_mut(|entry| {
                let retain = keep(&entry.key, &mut entry.value);
                // Mark before Vec drops the entry: its destructor may panic.
                *changed |= !retain;
                retain
            });
        }
    }

    fn retain_by_removal<F>(&mut self, mut keep: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let mut index = 0;
        while index < self.entries.len() {
            let entry = &mut self.entries[index];
            if keep(&entry.key, &mut entry.value) {
                index += 1;
                continue;
            }
            let bucket = entry.hash as usize & (self.buckets.len() - 1);
            let mut previous = EMPTY;
            let mut current = self.buckets[bucket].head;
            while current != index {
                previous = current;
                current = self.entries[current].next;
            }
            // Repair before dropping members so either destructor or the
            // next predicate may unwind without leaving stale bucket links.
            self.remove_linked_entry(index, bucket, previous);
            // swap_remove moved an unvisited entry into this index.
        }
    }

    /// Remove a dense entry, moving the last entry into its index.
    /// Uses its cached hash and takes expected O(1), without hashing the key.
    /// Indices are invalidated by removals; out-of-range indices change nothing.
    pub(crate) fn swap_remove_index(&mut self, index: usize) -> Option<(K, V)> {
        let hash = self.entries.get(index)?.hash;
        let bucket = hash as usize & (self.buckets.len() - 1);
        let mut previous = EMPTY;
        let mut current = self.buckets[bucket].head;
        while current != index {
            previous = current;
            current = self.entries[current].next;
        }
        Some(self.remove_linked_entry(index, bucket, previous))
    }

    #[inline]
    fn remove_linked_entry(&mut self, index: usize, bucket: usize, previous: usize) -> (K, V) {
        let next = self.entries[index].next;
        if previous == EMPTY {
            self.buckets[bucket].head = next;
        } else {
            self.entries[previous].next = next;
        }
        let removed = self.entries.swap_remove(index);
        if index < self.entries.len() {
            // The former last entry is now at index. Only its incoming link
            // needs repair; cached hashes avoid touching the key bytes again.
            let old_index = self.entries.len();
            let moved_bucket = self.entries[index].hash as usize & (self.buckets.len() - 1);
            if self.buckets[moved_bucket].head == old_index {
                self.buckets[moved_bucket].head = index;
            } else {
                let mut previous = self.buckets[moved_bucket].head;
                while self.entries[previous].next != old_index {
                    previous = self.entries[previous].next;
                }
                self.entries[previous].next = index;
            }
        }
        // Rebuild this short chain's exact filter, so repeated insert/delete
        // churn cannot saturate stale fingerprint bits and slow down misses.
        let mut fingerprints = 0;
        let mut current = self.buckets[bucket].head;
        while current != EMPTY {
            let entry = &self.entries[current];
            fingerprints |= fingerprint(entry.hash);
            current = entry.next;
        }
        self.buckets[bucket].fingerprints = fingerprints;
        (removed.key, removed.value)
    }

    fn rehash(&mut self, capacity: usize) {
        debug_assert!(capacity.is_power_of_two());
        let mut buckets = vec![EMPTY_BUCKET; capacity];
        let mask = capacity - 1;
        for (index, entry) in self.entries.iter_mut().enumerate() {
            let bucket = entry.hash as usize & mask;
            entry.next = buckets[bucket].head;
            buckets[bucket].head = index;
            buckets[bucket].fingerprints |= fingerprint(entry.hash);
        }
        self.buckets = buckets;
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter(self.entries.iter())
    }

    pub fn iter_mut(&mut self) -> impl ExactSizeIterator<Item = (&K, &mut V)> + '_ {
        self.entries
            .iter_mut()
            .map(|entry| (&entry.key, &mut entry.value))
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &K> + DoubleEndedIterator + '_ {
        self.entries.iter().map(|entry| &entry.key)
    }

    pub fn values(&self) -> impl ExactSizeIterator<Item = &V> + DoubleEndedIterator + '_ {
        self.entries.iter().map(|entry| &entry.value)
    }

    pub fn values_mut(&mut self) -> impl ExactSizeIterator<Item = &mut V> + '_ {
        self.entries.iter_mut().map(|entry| &mut entry.value)
    }

    /// Visits complete buckets in bit-reversed order and returns the next cursor.
    ///
    /// Start at zero, pass each returned cursor to the next call, and stop when
    /// zero is returned. `count` is a work hint: a collision chain is never split,
    /// so a page can exceed it. Sparse scans visit at most 10 * count buckets.
    /// Visitors may filter entries; the budget counts entries before filtering.
    ///
    /// Bucket counts are powers of two and rehashing preserves field hashes.
    /// Bit reversal visits newly split buckets before advancing past their old
    /// bucket. Shrinking revisits a merged bucket that includes the cursor's
    /// unvisited suffix, so duplicates are possible but coverage is preserved.
    /// Fields present throughout a full scan cannot be skipped by resizing or
    /// deletion of other fields, even though dense entries move on deletion.
    /// The caller must exclude writes for this call, not for the whole iteration.
    pub fn scan<F>(&self, mut cursor: u64, count: usize, mut visit: F) -> u64
    where
        F: FnMut(&K, &V),
    {
        if self.is_empty() {
            return 0;
        }
        let count = count.max(1);
        let max_buckets = count.saturating_mul(10);
        let mask = (self.buckets.len() - 1) as u64;
        let mut visited = 0;
        for _ in 0..max_buckets {
            let mut index = self.buckets[(cursor & mask) as usize].head;
            while index != EMPTY {
                let entry = &self.entries[index];
                visit(&entry.key, &entry.value);
                visited += 1;
                index = entry.next;
            }
            // Set bits outside the current mask before incrementing reversed
            // bits. This also normalizes arbitrary client-supplied cursors.
            cursor = (cursor | !mask)
                .reverse_bits()
                .wrapping_add(1)
                .reverse_bits();
            if cursor == 0 || visited >= count {
                break;
            }
        }
        cursor
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, const DENSE_BUCKETS: bool> ScanMap<K, V, S, DENSE_BUCKETS> {
    #[inline]
    fn find<Q>(&self, key: &Q, hash: u64) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        if self.buckets.is_empty() {
            return None;
        }
        let bucket = &self.buckets[hash as usize & (self.buckets.len() - 1)];
        if bucket.fingerprints & fingerprint(hash) == 0 {
            return None;
        }
        let mut index = bucket.head;
        while let Some(entry) = self.entries.get(index) {
            if entry.hash == hash && entry.key.borrow() == key {
                return Some(index);
            }
            index = entry.next;
        }
        None
    }

    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key, self.hash_builder.hash_one(key))
            .map(|index| &self.entries[index].value)
    }

    #[inline]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let index = self.find(key, self.hash_builder.hash_one(key))?;
        Some(&mut self.entries[index].value)
    }

    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key, self.hash_builder.hash_one(key)).is_some()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash = self.hash_builder.hash_one(&key);
        if let Some(index) = self.find(&key, hash) {
            return Some(std::mem::replace(&mut self.entries[index].value, value));
        }
        if self.len() == Self::bucket_capacity(self.buckets.len())
            || self.len() == self.entries.capacity()
        {
            self.reserve(1);
        }
        let bucket = hash as usize & (self.buckets.len() - 1);
        let next = self.buckets[bucket].head;
        self.buckets[bucket].head = self.entries.len();
        self.buckets[bucket].fingerprints |= fingerprint(hash);
        self.entries.push(Entry {
            hash,
            next,
            key,
            value,
        });
        None
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.remove_entry(key).map(|(_, value)| value)
    }

    pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.is_empty() {
            return None;
        }
        let hash = self.hash_builder.hash_one(key);
        let bucket = hash as usize & (self.buckets.len() - 1);
        if self.buckets[bucket].fingerprints & fingerprint(hash) == 0 {
            return None;
        }
        let mut index = self.buckets[bucket].head;
        let mut previous = EMPTY;
        while index != EMPTY {
            let entry = &self.entries[index];
            if entry.hash == hash && entry.key.borrow() == key {
                break;
            }
            previous = index;
            index = entry.next;
        }
        if index == EMPTY {
            return None;
        }
        Some(self.remove_linked_entry(index, bucket, previous))
    }
}

impl<K, V, S: Default, const DENSE_BUCKETS: bool> Default for ScanMap<K, V, S, DENSE_BUCKETS> {
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K: fmt::Debug, V: fmt::Debug, S, const DENSE_BUCKETS: bool> fmt::Debug
    for ScanMap<K, V, S, DENSE_BUCKETS>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, Q: Hash + Eq + ?Sized, const DENSE_BUCKETS: bool> Index<&Q>
    for ScanMap<K, V, S, DENSE_BUCKETS>
where
    K: Borrow<Q>,
{
    type Output = V;

    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).expect("no entry found for key")
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, const DENSE_BUCKETS: bool> Extend<(K, V)>
    for ScanMap<K, V, S, DENSE_BUCKETS>
{
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher + Default, const DENSE_BUCKETS: bool> FromIterator<(K, V)>
    for ScanMap<K, V, S, DENSE_BUCKETS>
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut map = Self::default();
        map.extend(iter);
        map
    }
}

impl<K: Eq + Hash, V, const N: usize> From<[(K, V); N]> for ScanMap<K, V> {
    fn from(entries: [(K, V); N]) -> Self {
        entries.into_iter().collect()
    }
}

pub struct Iter<'a, K, V>(std::slice::Iter<'a, Entry<K, V>>);

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|entry| (&entry.key, &entry.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> DoubleEndedIterator for Iter<'_, K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|entry| (&entry.key, &entry.value))
    }
}

impl<K, V> ExactSizeIterator for Iter<'_, K, V> {}
impl<K, V> FusedIterator for Iter<'_, K, V> {}

impl<'a, K, V, S, const DENSE_BUCKETS: bool> IntoIterator for &'a ScanMap<K, V, S, DENSE_BUCKETS> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct IntoIter<K, V>(std::vec::IntoIter<Entry<K, V>>);

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|entry| (entry.key, entry.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K, V> ExactSizeIterator for IntoIter<K, V> {}
impl<K, V> FusedIterator for IntoIter<K, V> {}

impl<K, V, S, const DENSE_BUCKETS: bool> IntoIterator for ScanMap<K, V, S, DENSE_BUCKETS> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.entries.into_iter())
    }
}

// Persist logical entries only. Hash seeds, bucket links and allocation sizes
// are local implementation details, as with std HashMap. Restoring a snapshot
// may change ordinary scan cursors; replicated reads use their canonical path.
impl<K: Serialize, V: Serialize, S, const DENSE_BUCKETS: bool> Serialize
    for ScanMap<K, V, S, DENSE_BUCKETS>
{
    fn serialize<T: Serializer>(&self, serializer: T) -> Result<T::Ok, T::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de, K, V, S, const DENSE_BUCKETS: bool> Deserialize<'de> for ScanMap<K, V, S, DENSE_BUCKETS>
where
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
    S: BuildHasher + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapVisitor<K, V, S, const DENSE_BUCKETS: bool>(PhantomData<(K, V, S)>);

        impl<'de, K, V, S, const DENSE_BUCKETS: bool> Visitor<'de> for MapVisitor<K, V, S, DENSE_BUCKETS>
        where
            K: Deserialize<'de> + Eq + Hash,
            V: Deserialize<'de>,
            S: BuildHasher + Default,
        {
            type Value = ScanMap<K, V, S, DENSE_BUCKETS>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
                // Do not trust an unvalidated serialized length enough to make
                // an unbounded initial allocation. Further growth follows data.
                let mut map = ScanMap::with_capacity_and_hasher(
                    input.size_hint().unwrap_or(0).min(4096),
                    S::default(),
                );
                while let Some((key, value)) = input.next_entry()? {
                    map.insert(key, value);
                }
                Ok(map)
            }
        }

        deserializer.deserialize_map(MapVisitor::<K, V, S, DENSE_BUCKETS>(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::hash::{BuildHasherDefault, Hasher};

    #[derive(Default)]
    struct IdentityHasher(u64);

    impl Hasher for IdentityHasher {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 = self.0.wrapping_mul(31).wrapping_add(u64::from(*byte));
            }
        }

        fn write_u64(&mut self, value: u64) {
            self.0 = value;
        }
    }

    type PredictableMap = ScanMap<u64, u64, BuildHasherDefault<IdentityHasher>>;
    type BucketMap<const DENSE: bool> =
        ScanMap<u64, u64, BuildHasherDefault<IdentityHasher>, DENSE>;

    fn finish_resized_scan<const DENSE: bool>(
        map: &BucketMap<DENSE>,
        mut cursor: u64,
        seen: &mut HashSet<u64>,
    ) {
        for _ in 0..=map.buckets.len() {
            cursor = map.scan(cursor, 1, |key, _| {
                seen.insert(*key);
            });
            if cursor == 0 {
                return;
            }
        }
        panic!("bounded resized table must finish scanning");
    }

    fn assert_matches(map: &PredictableMap, reference: &HashMap<u64, u64>) {
        assert_eq!(map.len(), reference.len());
        assert_eq!(
            map.iter().map(|(&k, &v)| (k, v)).collect::<HashMap<_, _>>(),
            *reference
        );
        for (&key, value) in reference {
            assert_eq!(map.get(&key), Some(value));
        }
        for bucket in &map.buckets {
            let mut bits = 0;
            let mut index = bucket.head;
            while index != EMPTY {
                bits |= fingerprint(map.entries[index].hash);
                index = map.entries[index].next;
            }
            assert_eq!(bucket.fingerprints, bits);
        }
    }

    #[test]
    fn mixed_operations_match_std_and_repair_colliding_moved_entries() {
        let mut map = PredictableMap::default();
        let mut reference = HashMap::new();
        let mut random = 0x1234_5678_90ab_cdef_u64;
        for step in 0..20_000_u64 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            // Most keys share low bits, stressing chain unlinking and repairs.
            let key = ((random >> 8) % 128) * 64;
            match random % 5 {
                0 | 1 => assert_eq!(map.insert(key, step), reference.insert(key, step)),
                2 => assert_eq!(map.remove(&key), reference.remove(&key)),
                3 => assert_eq!(map.get(&key), reference.get(&key)),
                _ => {
                    if let Some(value) = map.get_mut(&key) {
                        *value += 1;
                    }
                    if let Some(value) = reference.get_mut(&key) {
                        *value += 1;
                    }
                }
            }
            if step % 113 == 0 {
                assert_matches(&map, &reference);
            }
        }
        assert_matches(&map, &reference);
        for key in reference.keys().copied().collect::<Vec<_>>() {
            assert_eq!(map.remove(&key), reference.remove(&key));
            assert_matches(&map, &reference);
        }
        assert!(map.is_empty());
        assert!(map.capacity() > 0, "deletion must not automatically shrink");
    }

    #[test]
    fn indexed_removal_preserves_dense_order_and_repairs_collision_chains() {
        let mut expected: Vec<_> = (0..128_u64)
            .map(|key| (((key % 4) << 58) | (key * 1024) | (key % 3), key))
            .collect();
        let mut map: PredictableMap = expected.iter().copied().collect();
        let mut reference: HashMap<_, _> = expected.iter().copied().collect();
        let allocated = map.allocated_bytes();
        let mut step = 0;
        while !expected.is_empty() {
            assert_eq!(map.swap_remove_index(expected.len()), None);
            assert_eq!(map.swap_remove_index(usize::MAX), None);
            let index = match step % 3 {
                0 => 0,
                1 => expected.len() - 1,
                _ => expected.len() / 2,
            };
            let removed = expected.swap_remove(index);
            assert_eq!(map.swap_remove_index(index), Some(removed));
            reference.remove(&removed.0);
            assert_eq!(map.allocated_bytes(), allocated);
            assert_eq!(
                map.iter()
                    .map(|(&key, &value)| (key, value))
                    .collect::<Vec<_>>(),
                expected
            );
            assert_matches(&map, &reference);
            let mut scanned = HashSet::new();
            assert_eq!(
                map.scan(0, usize::MAX, |key, _| {
                    assert!(scanned.insert(*key));
                }),
                0
            );
            assert_eq!(scanned, reference.keys().copied().collect());
            step += 1;
        }
        assert_eq!(map.swap_remove_index(0), None);
        map.insert(7, 9);
        assert_eq!(map.swap_remove_index(0), Some((7, 9)));
    }

    #[test]
    fn retain_visits_each_entry_once_and_repairs_collisions_without_allocating() {
        for (capacity, swap_remove) in [(64, false), (4096, false), (64, true), (4096, true)] {
            let mut map = PredictableMap::with_capacity_and_hasher(capacity, Default::default());
            map.extend((0..64_u64).map(|key| (((key % 4) << 58) | (key * 1024), key)));
            let mut reference: HashMap<_, _> = map.iter().map(|(&k, &v)| (k, v)).collect();
            let allocated = map.allocated_bytes();
            let mut visited = HashSet::new();
            let keep = |key: &u64, value: &mut u64| {
                assert!(
                    visited.insert(*key),
                    "moved entries are visited exactly once"
                );
                *value += 1;
                *value % 3 != 0
            };
            if swap_remove {
                map.retain_swap_remove(keep);
            } else {
                map.retain(keep);
            }
            reference.retain(|_, value| {
                *value += 1;
                *value % 3 != 0
            });
            assert_eq!(visited.len(), 64);
            assert_eq!(map.allocated_bytes(), allocated);
            assert_matches(&map, &reference);
            if swap_remove {
                map.retain_swap_remove(|_, _| false);
            } else {
                map.retain(|_, _| false);
            }
            assert_matches(&map, &HashMap::new());
            assert_eq!(map.allocated_bytes(), allocated);
            map.insert(1024, 42);
            assert_eq!(map.remove(&1024), Some(42));
        }
    }

    #[test]
    fn retain_keeps_bucket_links_valid_when_predicate_panics() {
        for (capacity, swap_remove) in [(64, false), (4096, false), (64, true), (4096, true)] {
            let mut map = PredictableMap::with_capacity_and_hasher(capacity, Default::default());
            map.extend((0..64_u64).map(|key| (key * 1024, key)));
            let mut reference: HashMap<_, _> = map.iter().map(|(&k, &v)| (k, v)).collect();
            let mut visited = 0;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let keep = |key: &u64, value: &mut u64| {
                    visited += 1;
                    *value += 1;
                    *reference.get_mut(key).unwrap() = *value;
                    assert!(visited != 17, "predicate panic after mutating value");
                    if *value % 2 == 0 {
                        true
                    } else {
                        reference.remove(key);
                        false
                    }
                };
                if swap_remove {
                    map.retain_swap_remove(keep);
                } else {
                    map.retain(keep);
                }
            }));
            assert!(result.is_err());
            assert_matches(&map, &reference);
            map.insert(65 * 1024, 65);
            reference.insert(65 * 1024, 65);
            assert_matches(&map, &reference);
            for key in reference.keys().copied().collect::<Vec<_>>() {
                assert_eq!(map.remove(&key), reference.remove(&key));
            }
            assert!(map.is_empty());
        }
    }

    #[test]
    fn retain_rebuilds_links_when_a_removed_member_destructor_panics() {
        use std::cell::Cell;
        use std::rc::Rc;

        struct DropKey {
            id: u64,
            armed: Rc<Cell<bool>>,
        }

        impl Hash for DropKey {
            fn hash<H: Hasher>(&self, hasher: &mut H) {
                self.id.hash(hasher);
            }
        }
        impl PartialEq for DropKey {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }
        impl Eq for DropKey {}
        impl Borrow<u64> for DropKey {
            fn borrow(&self) -> &u64 {
                &self.id
            }
        }
        impl Drop for DropKey {
            fn drop(&mut self) {
                if self.id == 6 * 1024 && self.armed.replace(false) {
                    panic!("member destructor panic");
                }
            }
        }

        for (capacity, swap_remove) in [(12, false), (4096, false), (12, true), (4096, true)] {
            let armed = Rc::new(Cell::new(true));
            let mut map: ScanMap<DropKey, u64, BuildHasherDefault<IdentityHasher>> =
                ScanMap::with_capacity_and_hasher(capacity, Default::default());
            map.extend((0..12_u64).map(|id| {
                (
                    DropKey {
                        id: id * 1024,
                        armed: armed.clone(),
                    },
                    id,
                )
            }));
            let mut reference: HashMap<_, _> = (0..12_u64).map(|id| (id, id)).collect();
            let allocated = map.allocated_bytes();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let keep = |key: &DropKey, value: &mut u64| {
                    let id = key.id / 1024;
                    *value += 100;
                    *reference.get_mut(&id).unwrap() = *value;
                    if id % 2 != 0 {
                        true
                    } else {
                        reference.remove(&id);
                        false
                    }
                };
                if swap_remove {
                    map.retain_swap_remove(keep);
                } else {
                    map.retain(keep);
                }
            }));
            assert!(result.is_err());
            assert_eq!(map.len(), reference.len());
            assert_eq!(map.allocated_bytes(), allocated);
            for id in 0..12_u64 {
                assert_eq!(map.get(&(id * 1024)), reference.get(&id));
            }
            let mut seen = HashSet::new();
            assert_eq!(
                map.scan(0, usize::MAX, |key, _| {
                    seen.insert(key.id / 1024);
                }),
                0
            );
            assert_eq!(seen, reference.keys().copied().collect());
            for id in seen {
                assert!(map.remove(&(id * 1024)).is_some());
            }
            assert!(map.is_empty());
        }
    }

    #[test]
    fn borrowed_queries_replacements_clone_and_iterators() {
        let mut map = ScanMap::from([("a".to_owned(), 1), ("b".to_owned(), 2)]);
        assert_eq!(map.insert("a".to_owned(), 3), Some(1));
        assert_eq!(map.get("a"), Some(&3));
        assert!(map.contains_key("b"));
        for value in map.values_mut() {
            *value += 10;
        }
        let mut copy = map.clone();
        copy.insert("c".to_owned(), 15);
        assert_eq!(map.len(), 2);
        assert_eq!(copy.len(), 3);
        assert_eq!(map.iter().len(), 2);
        assert_eq!(map.keys().len(), 2);
        assert_eq!(map.values().copied().sum::<i32>(), 25);
        assert_eq!(map.remove_entry("a"), Some(("a".to_owned(), 13)));
        let capacity = copy.capacity();
        copy.clear();
        assert_eq!(copy.capacity(), capacity);
        assert_eq!(copy.scan(999, 1, |_, _| panic!("empty map")), 0);
        copy.extend([("d".to_owned(), 4), ("d".to_owned(), 5)]);
        assert_eq!(
            copy.into_iter().collect::<Vec<_>>(),
            vec![("d".to_owned(), 5)]
        );
    }

    #[test]
    fn replacement_keeps_the_original_equal_key_like_std_hashmap() {
        #[derive(Clone, Debug)]
        struct Key(u64, &'static str);

        impl PartialEq for Key {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
        impl Eq for Key {}
        impl Hash for Key {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        let mut map = ScanMap::new();
        map.insert(Key(1, "original"), 10);
        assert_eq!(map.insert(Key(1, "replacement"), 20), Some(10));
        let mut cloned = map.clone();
        let (key, value) = cloned.remove_entry(&Key(1, "lookup")).unwrap();
        assert_eq!(key.1, "original");
        assert_eq!(value, 20);
        assert_eq!(map.get(&Key(1, "lookup")), Some(&20));
    }

    #[test]
    fn explicit_capacity_keeps_dense_storage_exact_then_grows_geometrically() {
        for capacity in [3, 5, 17, 10_000] {
            let mut map = PredictableMap::with_capacity_and_hasher(capacity, Default::default());
            assert_eq!(map.entries.capacity(), capacity);
            assert_eq!(map.capacity(), capacity);
            assert_eq!(map.buckets.len(), capacity.next_power_of_two() * 2);
            assert_eq!(
                map.allocated_bytes(),
                capacity * size_of::<Entry<u64, u64>>() + map.buckets.len() * size_of::<Bucket>()
            );
            let allocated = map.allocated_bytes();
            for key in 0..capacity as u64 {
                map.insert(key, key);
            }
            assert_eq!(map.allocated_bytes(), allocated);
            let bucket_count = map.buckets.len();
            map.insert(capacity as u64, capacity as u64);
            assert_eq!(map.buckets.len(), bucket_count);
            assert_eq!(map.entries.capacity(), capacity.next_power_of_two());
            for key in 0..=capacity as u64 {
                assert_eq!(map.get(&key), Some(&key));
            }
        }
    }

    #[test]
    fn set_bucket_density_preserves_requested_capacity_and_default_map_layout() {
        for (capacity, buckets) in [
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 4),
            (4, 8),
            (5, 8),
            (6, 8),
            (7, 16),
            (10_000, 16_384),
        ] {
            let mut map = BucketMap::<true>::with_capacity_and_hasher(capacity, Default::default());
            assert_eq!(map.buckets.len(), buckets);
            assert_eq!(map.capacity(), capacity);
            let allocated = map.allocated_bytes();
            for key in 0..capacity as u64 {
                map.insert(key, key);
            }
            assert_eq!(map.allocated_bytes(), allocated);
            map.insert(capacity as u64, capacity as u64);
            for key in 0..=capacity as u64 {
                assert_eq!(map.get(&key), Some(&key));
            }
        }
        let default = PredictableMap::with_capacity_and_hasher(10_000, Default::default());
        assert_eq!(default.buckets.len(), 32_768);
    }

    #[test]
    fn sparse_clone_shrink_covers_survivors_at_every_original_bucket_boundary() {
        fn check<const DENSE: bool>() {
            for members in [16, 64, 256] {
                let mut source: BucketMap<DENSE> =
                    (0..members as u64).map(|key| (key, key)).collect();
                let old_buckets = source.buckets.len();
                let old_bits = old_buckets.trailing_zeros();
                let survivors: HashSet<_> =
                    (0..members as u64).filter(|key| key % 7 == 0).collect();
                source.retain(|key, _| survivors.contains(key));
                let shrunk = source.clone_for_write();
                assert!(shrunk.buckets.len() < old_buckets);
                for boundary in 0..old_buckets {
                    // A completed fine-bucket prefix, including empty bucket
                    // boundaries that a particular COUNT need not expose.
                    let cursor = (boundary as u64).reverse_bits() >> (64 - old_bits);
                    let mut seen: HashSet<_> = survivors
                        .iter()
                        .copied()
                        .filter(|key| {
                            let bucket = key & (old_buckets as u64 - 1);
                            let rank = bucket.reverse_bits() >> (64 - old_bits);
                            rank < boundary as u64
                        })
                        .collect();
                    finish_resized_scan(&shrunk, cursor, &mut seen);
                    assert_eq!(seen, survivors, "{old_buckets} buckets at {boundary}");
                }
            }
        }
        check::<false>();
        check::<true>();
    }

    #[test]
    fn alternating_growth_and_shrink_preserve_stable_member_coverage() {
        fn check<const DENSE: bool>() {
            let mut map = BucketMap::<DENSE>::with_capacity_and_hasher(4096, Default::default());
            // Removing these earlier dense entries moves stable members too.
            map.extend((10_000..10_032_u64).map(|key| (key, key)));
            let expected: HashSet<_> = (0..64_u64).map(|key| key * 31).collect();
            map.extend(expected.iter().map(|key| (*key, *key)));
            let sizes = [256, 8192, 512, 4096, 128, 2048];
            let mut cursor = 0;
            let mut seen = HashSet::new();
            for step in 0..=8192 {
                cursor = map.scan(cursor, 1 + step % 3, |key, _| {
                    seen.insert(*key);
                });
                if cursor == 0 {
                    assert!(expected.is_subset(&seen));
                    return;
                }
                if step < 32 {
                    map.remove(&(10_000 + step as u64));
                }
                map.insert(1_000_000 + step as u64, 0);
                if step != 0 {
                    map.remove(&(999_999 + step as u64));
                }
                let mut minimum = map.len().max(1).next_power_of_two();
                if map.len() > BucketMap::<DENSE>::bucket_capacity(minimum) {
                    minimum *= 2;
                }
                map.rehash(sizes[step % sizes.len()].max(minimum));
            }
            panic!("bounded table with alternating resizes must finish");
        }
        check::<false>();
        check::<true>();
    }

    #[test]
    fn growth_and_dense_moves_do_not_skip_fields_present_throughout_scan() {
        for count in [1, 2, 7, 17] {
            let mut map: PredictableMap = (0..64).map(|key| (key, key)).collect();
            let survivors: HashSet<_> = (32..64).collect();
            let mut seen = HashSet::new();
            let mut cursor = 0;
            let mut calls = 0;
            loop {
                cursor = map.scan(cursor, count, |key, value| {
                    assert_eq!(key, value);
                    seen.insert(*key);
                });
                calls += 1;
                if cursor == 0 {
                    break;
                }
                if calls == 1 {
                    // Force several synchronous resizes after a partial scan.
                    for key in 64..1024 {
                        map.insert(key, key);
                    }
                    for key in 0..32 {
                        map.remove(&key);
                    }
                } else {
                    // Repeated insert/remove churn moves dense entries but
                    // cannot alter their logical scan bucket.
                    map.insert(2048 + calls, 2048 + calls);
                    map.remove(&(2047 + calls));
                }
                assert!(calls < 2048, "bounded table must terminate");
            }
            assert!(survivors.is_subset(&seen));
        }
    }

    #[test]
    fn growth_at_each_bucket_boundary_preserves_the_remaining_scan() {
        for boundary in 1..16 {
            for new_capacity in [32, 128, 512] {
                let mut map: PredictableMap = (0..16).map(|key| (key, key)).collect();
                let mut seen = HashSet::new();
                let mut cursor = 0;
                for _ in 0..boundary {
                    cursor = map.scan(cursor, 1, |key, _| {
                        seen.insert(*key);
                    });
                    assert_ne!(cursor, 0);
                }
                map.reserve(new_capacity);
                // Remove only fields already visited, including dense entries
                // before the unvisited ones. No surviving field may be skipped.
                for key in seen.iter().copied().collect::<Vec<_>>() {
                    map.remove(&key);
                }
                for key in 16..32 {
                    map.insert(key, key);
                }
                for call in 0..new_capacity {
                    cursor = map.scan(cursor, 1, |key, _| {
                        seen.insert(*key);
                    });
                    if cursor == 0 {
                        break;
                    }
                    assert!(call + 1 < new_capacity);
                }
                assert!((0..16).all(|key| seen.contains(&key)));
            }
        }
    }

    #[test]
    fn byte_keys_and_randomized_hashes_survive_resize_and_delete_churn() {
        for _ in 0..8 {
            let keys: Vec<bytes::Bytes> = (0..256_u64)
                .map(|key| bytes::Bytes::copy_from_slice(&key.to_le_bytes()))
                .collect();
            let mut map = ScanMap::with_capacity(1);
            for (index, key) in keys.iter().take(64).enumerate() {
                map.insert(key.clone(), index);
            }
            let survivors: HashSet<_> = keys[32..64].iter().cloned().collect();
            let mut seen = HashSet::new();
            let mut cursor = map.scan(0, 1, |key, _| {
                seen.insert(key.clone());
            });
            assert_ne!(cursor, 0);
            for (index, key) in keys.iter().enumerate().skip(64) {
                map.insert(key.clone(), index);
            }
            for key in &keys[..32] {
                map.remove(key.as_ref());
            }
            for call in 0..512 {
                cursor = map.scan(cursor, (call % 5) + 1, |key, _| {
                    seen.insert(key.clone());
                });
                if cursor == 0 {
                    break;
                }
                assert!(call < 511);
            }
            assert!(survivors.is_subset(&seen));
            for (index, key) in keys.iter().enumerate().skip(32) {
                assert_eq!(map.get(key.as_ref()), Some(&index));
            }
        }
    }

    #[test]
    fn cloned_sparse_map_can_insert_until_its_bucket_capacity_and_then_grow() {
        let mut map: PredictableMap = (0..128).map(|key| (key, key)).collect();
        for key in 0..120 {
            map.remove(&key);
        }
        let mut cloned = map.clone();
        // Vec::clone may reserve only len entries while bucket storage still
        // retains its old capacity. Insertion must grow entries independently.
        for key in 128..400 {
            cloned.insert(key, key);
        }
        assert_eq!(map.len(), 8);
        assert_eq!(cloned.len(), 280);
        for key in 120..400 {
            assert_eq!(cloned.get(&key), Some(&key));
        }
        let mut seen = HashSet::new();
        let mut cursor = 0;
        loop {
            cursor = cloned.scan(cursor, 3, |key, _| {
                seen.insert(*key);
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen, (120..400).collect());
    }

    #[test]
    fn scan_count_is_a_hint_and_sparse_work_is_bounded() {
        let mut collisions = PredictableMap::default();
        for key in [0, 64, 128, 192] {
            collisions.insert(key, key);
        }
        let mut fields = Vec::new();
        let cursor = collisions.scan(0, 1, |key, _| fields.push(*key));
        assert_eq!(fields.len(), 4, "never split a collision chain");
        assert_ne!(cursor, 0);

        let mut sparse = PredictableMap::with_capacity_and_hasher(4096, Default::default());
        sparse.insert(4095, 1);
        let cursor = sparse.scan(0, 1, |_, _| panic!("field is in the last bucket"));
        assert_ne!(cursor, 0, "empty page may still have a continuation");
        let mut seen = Vec::new();
        assert_eq!(sparse.scan(0, usize::MAX, |key, _| seen.push(*key)), 0);
        assert_eq!(seen, vec![4095]);
    }

    #[test]
    fn serde_roundtrip_restores_logical_entries_and_can_keep_mutating() {
        let mut map: ScanMap<String, u64> =
            (0..100).map(|key| (format!("field:{key}"), key)).collect();
        for key in 0..40 {
            map.remove(format!("field:{key}").as_str());
        }
        let bytes = bincode2::serialize(&map).unwrap();
        let mut restored: ScanMap<String, u64> = bincode2::deserialize(&bytes).unwrap();
        assert_eq!(restored.len(), map.len());
        for (key, value) in &map {
            assert_eq!(restored.get(key), Some(value));
        }
        restored.insert("after".into(), 1000);
        assert_eq!(restored.remove("field:99"), Some(99));
        let mut fields = HashSet::new();
        let mut cursor = 0;
        loop {
            cursor = restored.scan(cursor, 3, |key, _| {
                fields.insert(key.clone());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(fields, restored.keys().cloned().collect());
    }
}
