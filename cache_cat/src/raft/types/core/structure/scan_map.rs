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
#[derive(Clone)]
pub struct ScanMap<K, V, S = RandomState> {
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

impl<K, V, S> ScanMap<K, V, S> {
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
        (self.buckets.len() / 2).min(self.entries.capacity())
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
        if needed > self.buckets.len() / 2 {
            let capacity = needed
                .max(MIN_BUCKETS)
                .checked_next_power_of_two()
                .and_then(|capacity| capacity.checked_mul(2))
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

    /// Removes all entries while keeping the allocation and hash seed.
    pub fn clear(&mut self) {
        self.buckets.fill(EMPTY_BUCKET);
        self.entries.clear();
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
    /// bucket. Fields present throughout a full scan cannot be skipped by growth
    /// or deletion of other fields, even though dense entries move on deletion.
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

impl<K: Eq + Hash, V, S: BuildHasher> ScanMap<K, V, S> {
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
        if self.len() == self.buckets.len() / 2 || self.len() == self.entries.capacity() {
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
        Some((removed.key, removed.value))
    }
}

impl<K, V, S: Default> Default for ScanMap<K, V, S> {
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K: fmt::Debug, V: fmt::Debug, S> fmt::Debug for ScanMap<K, V, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Eq + Hash, V, S: BuildHasher, Q: Hash + Eq + ?Sized> Index<&Q> for ScanMap<K, V, S>
where
    K: Borrow<Q>,
{
    type Output = V;

    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).expect("no entry found for key")
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Extend<(K, V)> for ScanMap<K, V, S> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for (key, value) in iter {
            self.insert(key, value);
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher + Default> FromIterator<(K, V)> for ScanMap<K, V, S> {
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

impl<'a, K, V, S> IntoIterator for &'a ScanMap<K, V, S> {
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

impl<K, V, S> IntoIterator for ScanMap<K, V, S> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.entries.into_iter())
    }
}

// Persist logical entries only. Hash seeds, bucket links and allocation sizes
// are local implementation details, as with std HashMap. Restoring a snapshot
// may change ordinary scan cursors; replicated reads use their canonical path.
impl<K: Serialize, V: Serialize, S> Serialize for ScanMap<K, V, S> {
    fn serialize<T: Serializer>(&self, serializer: T) -> Result<T::Ok, T::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl<'de, K, V, S> Deserialize<'de> for ScanMap<K, V, S>
where
    K: Deserialize<'de> + Eq + Hash,
    V: Deserialize<'de>,
    S: BuildHasher + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct MapVisitor<K, V, S>(PhantomData<(K, V, S)>);

        impl<'de, K, V, S> Visitor<'de> for MapVisitor<K, V, S>
        where
            K: Deserialize<'de> + Eq + Hash,
            V: Deserialize<'de>,
            S: BuildHasher + Default,
        {
            type Value = ScanMap<K, V, S>;

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

        deserializer.deserialize_map(MapVisitor(PhantomData))
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
