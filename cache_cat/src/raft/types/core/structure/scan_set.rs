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

/// Snapshot clones preserve member hashes. Sparse clones may shrink buckets,
/// allowing repeated scan results while preserving coverage of stable members.
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

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
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
        self.0.retain(|member, ()| keep(member));
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

// Persist members only, using the same sequence representation as HashSet.
// Restoring rebuilds the local hash seed and scan layout.
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
