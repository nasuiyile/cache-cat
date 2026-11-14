//! Set operations and stateless bucket scans backed by the shared ScanMap.

use super::scan_map::{self, ScanMap};
use ahash::RandomState;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::marker::PhantomData;

/// Dense member order is part of replicated Set state: insertion appends and
/// removal swaps in the last member. Clone and serde preserve that order while
/// local hash seeds and bucket capacities may differ. Sparse clones can shrink
/// buckets, allowing repeated scan results without skipping stable members.
pub struct ScanSet<K, S = RandomState>(ScanMap<K, (), S, true>);

impl<K: Clone, S: Clone> Clone for ScanSet<K, S> {
    fn clone(&self) -> Self {
        Self(self.0.clone_for_write())
    }
}

impl<K> ScanSet<K> {
    pub fn new() -> Self {
        Self::with_hasher(RandomState::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, S> ScanSet<K, S> {
    pub fn with_hasher(hash_builder: S) -> Self {
        Self(ScanMap::with_hasher(hash_builder))
    }

    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        Self(ScanMap::with_capacity_and_hasher(capacity, hash_builder))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Read a member by its replicated dense index without changing its order.
    #[inline]
    pub(crate) fn get_index(&self, index: usize) -> Option<&K> {
        self.0.get_index(index).map(|(member, ())| member)
    }
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Apply a batch of removals, reclaiming sparse storage once on completion.
    /// The closure's result and the remaining members' dense order are preserved.
    /// No-op batches and empty results keep their allocations.
    pub(crate) fn with_removals<R>(&mut self, mutate: impl FnOnce(&mut Self) -> R) -> R {
        let before = self.len();
        let result = mutate(self);
        if self.len() < before && !self.is_empty() {
            self.shrink_if_sparse();
        }
        result
    }

    /// Reclaim sparse storage once after a deletion batch, preserving member order.
    fn shrink_if_sparse(&mut self) -> bool {
        self.0.shrink_if_sparse()
    }

    pub fn hasher(&self) -> &S {
        self.0.hasher()
    }

    /// Heap storage for the table, excluding allocations owned by members.
    pub fn allocated_bytes(&self) -> usize {
        self.0.allocated_bytes()
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&K) -> bool,
    {
        self.0.retain_swap_remove(|member, ()| keep(member));
    }

    /// Remove a member by its dense index with expected O(1) bucket repair.
    /// Moves the last member into the removed index, just like member removal.
    pub(crate) fn swap_remove_index(&mut self, index: usize) -> Option<K> {
        self.0.swap_remove_index(index).map(|(member, ())| member)
    }

    pub fn iter(&self) -> Iter<'_, K> {
        Iter(self.0.iter())
    }

    /// Visits complete buckets; COUNT is a work hint before any filtering.
    /// The caller excludes writes during this call, not the entire iteration.
    pub fn scan<F>(&self, cursor: u64, count: usize, mut visit: F) -> u64
    where
        F: FnMut(&K),
    {
        self.0.scan(cursor, count, |member, ()| visit(member))
    }
}

impl<K: Eq + Hash, S: BuildHasher> ScanSet<K, S> {
    #[inline]
    pub fn contains<Q>(&self, member: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.contains_key(member)
    }

    #[inline]
    pub fn insert(&mut self, member: K) -> bool {
        self.0.insert(member, ()).is_none()
    }

    #[inline]
    pub fn remove<Q>(&mut self, member: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.remove(member).is_some()
    }
}

impl<K, S: Default> Default for ScanSet<K, S> {
    fn default() -> Self {
        Self(ScanMap::default())
    }
}

impl<K: fmt::Debug, S> fmt::Debug for ScanSet<K, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_set().entries(self.iter()).finish()
    }
}

impl<K: Eq + Hash, S: BuildHasher, T: BuildHasher> PartialEq<ScanSet<K, T>> for ScanSet<K, S> {
    fn eq(&self, other: &ScanSet<K, T>) -> bool {
        self.len() == other.len() && self.iter().all(|member| other.contains(member))
    }
}

impl<K: Eq + Hash, S: BuildHasher> Eq for ScanSet<K, S> {}

impl<K: Eq + Hash, S: BuildHasher> Extend<K> for ScanSet<K, S> {
    fn extend<I: IntoIterator<Item = K>>(&mut self, iter: I) {
        self.0.extend(iter.into_iter().map(|member| (member, ())));
    }
}

impl<K: Eq + Hash, S: BuildHasher + Default> FromIterator<K> for ScanSet<K, S> {
    fn from_iter<I: IntoIterator<Item = K>>(iter: I) -> Self {
        let mut set = Self::default();
        set.extend(iter);
        set
    }
}

impl<K: Eq + Hash, const N: usize> From<[K; N]> for ScanSet<K> {
    fn from(members: [K; N]) -> Self {
        members.into_iter().collect()
    }
}

pub struct Iter<'a, K>(scan_map::Iter<'a, K, ()>);

impl<'a, K> Iterator for Iter<'a, K> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(member, ())| member)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K> DoubleEndedIterator for Iter<'_, K> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(member, ())| member)
    }
}

impl<K> ExactSizeIterator for Iter<'_, K> {}
impl<K> FusedIterator for Iter<'_, K> {}

impl<'a, K, S> IntoIterator for &'a ScanSet<K, S> {
    type Item = &'a K;
    type IntoIter = Iter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct IntoIter<K>(scan_map::IntoIter<K, ()>);

impl<K> Iterator for IntoIter<K> {
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(member, ())| member)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<K> ExactSizeIterator for IntoIter<K> {}
impl<K> FusedIterator for IntoIter<K> {}

impl<K, S> IntoIterator for ScanSet<K, S> {
    type Item = K;
    type IntoIter = IntoIter<K>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter(self.0.into_iter())
    }
}

// The member sequence preserves dense index order without extra metadata.
// Restoring inserts in that order while rebuilding local hashes and buckets.
impl<K: Serialize, S> Serialize for ScanSet<K, S> {
    fn serialize<T: Serializer>(&self, serializer: T) -> Result<T::Ok, T::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;
        for member in self {
            sequence.serialize_element(member)?;
        }
        sequence.end()
    }
}

impl<'de, K, S> Deserialize<'de> for ScanSet<K, S>
where
    K: Deserialize<'de> + Eq + Hash,
    S: BuildHasher + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SetVisitor<K, S>(PhantomData<(K, S)>);

        impl<'de, K, S> Visitor<'de> for SetVisitor<K, S>
        where
            K: Deserialize<'de> + Eq + Hash,
            S: BuildHasher + Default,
        {
            type Value = ScanSet<K, S>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a set member sequence")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> Result<Self::Value, A::Error> {
                // An untrusted length must not cause an unbounded allocation.
                let mut set = ScanSet::with_capacity_and_hasher(
                    input.size_hint().unwrap_or(0).min(4096),
                    S::default(),
                );
                while let Some(member) = input.next_element()? {
                    set.insert(member);
                }
                Ok(set)
            }
        }

        deserializer.deserialize_seq(SetVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashSet;

    #[test]
    fn set_operations_preserve_membership_and_borrowed_queries() {
        let mut set = ScanSet::with_capacity(8);
        assert!(set.insert("first".to_owned()));
        assert!(!set.insert("first".to_owned()));
        set.extend(["second".to_owned(), "third".to_owned()]);
        assert!(set.contains("second"));
        assert!(set.remove("second"));
        assert!(!set.remove("second"));
        assert_eq!(set.iter().len(), 2);
        assert_eq!(set, ScanSet::from(["third".to_owned(), "first".to_owned()]));
        let mut visited = HashSet::new();
        let allocated = set.allocated_bytes();
        set.retain(|member| {
            assert!(visited.insert(member.clone()));
            member.starts_with('f')
        });
        assert_eq!(visited.len(), 2);
        assert_eq!(set.allocated_bytes(), allocated);
        assert_eq!(set.into_iter().collect::<Vec<_>>(), vec!["first"]);

        let mut set = ScanSet::from([1, 2, 3]);
        let capacity = set.capacity();
        set.clear();
        assert_eq!(set.capacity(), capacity);
        assert_eq!(set.scan(42, 1, |_| panic!("empty set")), 0);
    }

    #[test]
    fn retain_order_is_independent_of_hash_seed_and_bucket_capacity() {
        for capacity in [4, 4096] {
            let mut set = ScanSet::with_capacity(capacity);
            set.extend(['a', 'b', 'c', 'd']);
            set.retain(|member| *member != 'a');
            assert_eq!(set.into_iter().collect::<Vec<_>>(), ['d', 'b', 'c']);
        }

        let mut replicas: Vec<_> = [0, 64, 4096]
            .into_iter()
            .enumerate()
            .map(|(replica, capacity)| {
                let seed = replica as u64;
                let mut set = ScanSet::with_capacity_and_hasher(
                    capacity,
                    RandomState::with_seeds(seed, seed + 1, seed + 2, seed + 3),
                );
                set.extend(0..64_u64);
                set
            })
            .collect();
        let mut members: HashSet<_> = (0..64_u64).collect();
        for divisor in [2, 3, 5, 7] {
            members.retain(|member| member % divisor != 0);
            let mut expected_order = None;
            let mut expected_visits = None;
            for set in &mut replicas {
                let allocated = set.allocated_bytes();
                let mut visits = Vec::new();
                set.retain(|member| {
                    visits.push(*member);
                    member % divisor != 0
                });
                assert_eq!(
                    visits.iter().copied().collect::<HashSet<_>>().len(),
                    visits.len()
                );
                assert_eq!(set.allocated_bytes(), allocated);
                assert_eq!(set.iter().copied().collect::<HashSet<_>>(), members);
                let order = set.iter().copied().collect::<Vec<_>>();
                if let Some(expected) = &expected_order {
                    assert_eq!(&order, expected);
                    assert_eq!(&visits, expected_visits.as_ref().unwrap());
                } else {
                    expected_order = Some(order);
                    expected_visits = Some(visits);
                }
            }
        }
    }

    #[test]
    fn clone_and_serde_preserve_dense_order_for_later_indexed_removals() {
        let mut original = ScanSet::with_capacity(4096);
        original.extend(0..128_u64);
        for member in (0..96).step_by(2) {
            assert!(original.remove(&member));
        }
        let mut cloned = original.clone();
        assert!(cloned.allocated_bytes() < original.allocated_bytes());
        let encoded = bincode2::serialize(&original).unwrap();
        let mut restored: ScanSet<u64> = bincode2::deserialize(&encoded).unwrap();
        let order = original.iter().copied().collect::<Vec<_>>();
        assert_eq!(cloned.iter().copied().collect::<Vec<_>>(), order);
        assert_eq!(restored.iter().copied().collect::<Vec<_>>(), order);

        for set in [&mut original, &mut cloned, &mut restored] {
            assert!(!set.insert(127));
            assert!(set.insert(200));
            assert!(!set.remove(&999));
            set.retain(|member| member % 3 != 0);
        }
        let mut step = 0;
        while !original.is_empty() {
            let order = original.iter().copied().collect::<Vec<_>>();
            assert_eq!(cloned.iter().copied().collect::<Vec<_>>(), order);
            assert_eq!(restored.iter().copied().collect::<Vec<_>>(), order);
            let index = (step * 17 + 3) % original.len();
            let removed = original.swap_remove_index(index);
            assert_eq!(cloned.swap_remove_index(index), removed);
            assert_eq!(restored.swap_remove_index(index), removed);
            step += 1;
        }
        for set in [&mut original, &mut cloned, &mut restored] {
            assert_eq!(set.swap_remove_index(0), None);
            assert!(set.is_empty());
            assert!(set.insert(300));
            assert_eq!(set.swap_remove_index(0), Some(300));
        }
    }

    #[test]
    fn sparse_shrink_and_unchanged_capacity_keep_identical_member_indices() {
        let mut source = ScanSet::with_capacity(32_768);
        source.extend((0..256_u64).map(|member| member * 37));
        for member in 0..64 {
            assert!(source.remove(&(member * 37)));
        }
        let encoded = bincode2::serialize(&source).unwrap();
        let mut replica: ScanSet<u64> = bincode2::deserialize(&encoded).unwrap();
        let order = source.iter().copied().collect::<Vec<_>>();
        let before_bytes = source.allocated_bytes();
        assert!(source.shrink_if_sparse());
        assert!(source.allocated_bytes() < before_bytes / 4);
        assert_eq!(source.iter().copied().collect::<Vec<_>>(), order);
        assert!(!replica.shrink_if_sparse());
        for step in 0..64 {
            let index = (step * 17 + 3) % source.len();
            assert_eq!(
                source.swap_remove_index(index),
                replica.swap_remove_index(index)
            );
            assert_eq!(
                source.iter().copied().collect::<Vec<_>>(),
                replica.iter().copied().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn removal_batch_preserves_result_and_defers_reclamation_until_completion() {
        let mut set = ScanSet::with_capacity(32_768);
        set.extend(0..128_u64);
        let allocated = set.allocated_bytes();
        assert!(!set.with_removals(|set| set.remove(&999)));
        assert_eq!(set.allocated_bytes(), allocated);
        let remaining = set.with_removals(|set| {
            for member in 0..120 {
                assert!(set.remove(&member));
                assert_eq!(set.allocated_bytes(), allocated);
            }
            set.iter().copied().collect::<Vec<_>>()
        });
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), remaining);
        assert!(set.allocated_bytes() < allocated / 4);

        let mut set = ScanSet::with_capacity(32_768);
        set.extend(0..128_u64);
        let allocated = set.allocated_bytes();
        assert_eq!(
            set.with_removals(|set| {
                set.retain(|_| false);
                set.len()
            }),
            0
        );
        assert_eq!(set.allocated_bytes(), allocated);
    }

    #[test]
    fn clone_preserves_every_scan_page_when_bucket_layout_is_dense() {
        let mut original: ScanSet<_> = (0..256).collect();
        for member in 0..32 {
            original.remove(&member);
        }
        let cloned = original.clone();
        let mut cursor = 0;
        for _ in 0..512 {
            let mut left = Vec::new();
            let mut right = Vec::new();
            let next = original.scan(cursor, 1, |member| left.push(*member));
            assert_eq!(cloned.scan(cursor, 1, |member| right.push(*member)), next);
            assert_eq!(left, right);
            cursor = next;
            if cursor == 0 {
                return;
            }
        }
        panic!("scan must terminate");
    }

    #[test]
    fn sparse_clone_reduces_allocations_and_continues_each_original_page() {
        let mut original = ScanSet::with_capacity(4096);
        original.extend((0..128_u64).map(|member| member * 73));
        let expected: HashSet<_> = original.iter().copied().collect();
        let mut cursor = 0;
        let mut prefix = HashSet::new();
        for _ in 0..2048 {
            let cloned = original.clone();
            assert!(cloned.allocated_bytes() < original.allocated_bytes() / 2);
            assert_eq!(
                original.hasher().hash_one(12345),
                cloned.hasher().hash_one(12345)
            );
            let mut seen = prefix.clone();
            let mut continuation = cursor;
            for _ in 0..512 {
                continuation = cloned.scan(continuation, 3, |member| {
                    seen.insert(*member);
                });
                if continuation == 0 {
                    break;
                }
            }
            assert_eq!(continuation, 0);
            assert_eq!(seen, expected);

            cursor = original.scan(cursor, 3, |member| {
                prefix.insert(*member);
            });
            if cursor == 0 {
                assert_eq!(prefix, expected);
                return;
            }
        }
        panic!("original scan must terminate");
    }

    #[test]
    fn clone_keeps_growth_slack_without_reserving_historical_capacity() {
        for (reserved, len) in [(5, 5), (10_000, 10_000), (16_384, 10_000), (4096, 625)] {
            let mut original = ScanSet::with_capacity(reserved);
            original.extend(0..len);
            let mut cloned = original.clone();
            let capacity = cloned.capacity();
            let allocated = cloned.allocated_bytes();
            assert!(capacity > len);
            assert!(capacity <= (len + 1).next_power_of_two());
            assert!(cloned.insert(len));
            assert_eq!(cloned.capacity(), capacity);
            assert_eq!(cloned.allocated_bytes(), allocated);
            assert!(!original.contains(&len));
        }

        let mut empty = ScanSet::<u64>::with_capacity(4096);
        empty.insert(1);
        empty.clear();
        let mut cloned = empty.clone();
        assert_eq!(cloned.capacity(), 0);
        assert_eq!(cloned.allocated_bytes(), 0);
        assert_eq!(cloned.scan(42, 1, |_| panic!("empty set")), 0);
        assert!(cloned.insert(1));
    }

    #[test]
    fn serde_uses_member_sequences_and_restored_sets_can_mutate_and_scan() {
        let members = vec![
            Bytes::new(),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"\0\xff"),
        ];
        let set: ScanSet<_> = members.iter().cloned().collect();
        let encoded = bincode2::serialize(&set).unwrap();
        let decoded: Vec<Bytes> = bincode2::deserialize(&encoded).unwrap();
        assert_eq!(
            decoded.into_iter().collect::<HashSet<_>>(),
            members.iter().cloned().collect()
        );

        let legacy: HashSet<_> = members.iter().cloned().collect();
        let mut restored: ScanSet<Bytes> =
            bincode2::deserialize(&bincode2::serialize(&legacy).unwrap()).unwrap();
        assert_eq!(restored, set);
        assert!(restored.remove(b"a".as_slice()));
        assert!(restored.insert(Bytes::from_static(b"after")));
        let mut seen = HashSet::new();
        assert_eq!(
            restored.scan(0, usize::MAX, |member| {
                seen.insert(member.clone());
            }),
            0
        );
        assert_eq!(seen, restored.into_iter().collect());
        let duplicates: ScanSet<u64> = serde_json::from_str("[1,2,1]").unwrap();
        assert_eq!(duplicates, ScanSet::from([1, 2]));
    }
}
