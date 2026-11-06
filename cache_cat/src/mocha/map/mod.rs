//! Concurrent hash table with stateless, resize-safe Redis cursors.
//!
//! The storage/reclamation core is derived from Papaya 0.2.4 (MIT, see
//! LICENSE-MIT). Point operations retain its entry/slot layout: one byte
//! of metadata and one pointer per slot, and one allocation per key/value.
//! SCAN visits logical hash buckets, without a second index or per-entry scan
//! metadata. The Raft writer can run alongside readers, physical expiration,
//! and lazy deletion; these auxiliary writers remain safe.
//! Migration is incremental only. Waiting uses shared address queues instead
//! of a mutex and waiter registry in each table; the allocation lock prevents
//! competing helpers from allocating duplicate successor tables.
//!
//! Guards protect references and obsolete tables. An owned guard may cross
//! await points; a local guard must remain on its originating thread. Dropping
//! guards promptly permits reclamation. Neither iteration nor SCAN is an MVCC
//! snapshot; the cache's existing revision/delta protocol provides snapshots.

#![allow(dead_code, unstable_name_collisions)]
#![deny(unsafe_op_in_unsafe_fn)]

mod raw;
#[cfg(test)]
mod tests;

pub use equivalent::Equivalent;
pub use seize::LocalGuard;
use seize::{Collector, Guard, OwnedGuard};
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hash};

use raw::utils::MapGuard;

pub struct HashMap<K, V, S = RandomState> {
    raw: raw::HashMap<K, V, S>,
}

// SAFETY: The map owns its collector. Its references never outlive a guard
// borrowing the map. Moving the map moves ownership of its keys/values.
unsafe impl<K: Send, V: Send, S: Send> Send for HashMap<K, V, S> {}
// SAFETY: Shared access only exposes shared K/V references. Updates publish
// immutable entries atomically; retirement can drop them on another thread.
unsafe impl<K: Send + Sync, V: Send + Sync, S: Sync> Sync for HashMap<K, V, S> {}

impl<K, V, S> fmt::Debug for HashMap<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashMap")
            .field("len", &self.raw.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operation<V, T> {
    Insert(V),
    Remove,
    Abort(T),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Compute<'g, K, V, T> {
    Inserted(&'g K, &'g V),
    Updated {
        old: (&'g K, &'g V),
        new: (&'g K, &'g V),
    },
    Removed(&'g K, &'g V),
    Aborted(T),
}

#[derive(Debug)]
enum ResizeMode {
    Incremental(usize),
}

impl<K, V> HashMap<K, V> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, V, S: Default> Default for HashMap<K, V, S> {
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<K, V, S> HashMap<K, V, S> {
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self {
        Self {
            raw: raw::HashMap::new(
                capacity,
                hasher,
                Collector::default(),
                ResizeMode::Incremental(64),
            ),
        }
    }

    #[inline]
    pub fn pin(&self) -> HashMapRef<'_, K, V, S, LocalGuard<'_>> {
        HashMapRef {
            guard: self.raw.guard(),
            map: self,
        }
    }

    #[inline]
    pub fn pin_owned(&self) -> HashMapRef<'_, K, V, S, OwnedGuard<'_>> {
        HashMapRef {
            guard: self.raw.owned_guard(),
            map: self,
        }
    }

    #[inline]
    pub fn guard(&self) -> LocalGuard<'_> {
        self.raw.collector().enter()
    }
}

pub struct HashMapRef<'map, K, V, S, G> {
    guard: MapGuard<G>,
    map: &'map HashMap<K, V, S>,
}

impl<K: Hash + Eq, V, S: BuildHasher, G: Guard> HashMapRef<'_, K, V, S, G> {
    #[inline]
    pub fn len(&self) -> usize {
        self.map.raw.len()
    }

    #[inline]
    pub fn get<Q: Equivalent<K> + Hash + ?Sized>(&self, key: &Q) -> Option<&V> {
        self.get_key_value(key).map(|(_, value)| value)
    }

    #[inline]
    pub fn get_key_value<Q: Equivalent<K> + Hash + ?Sized>(&self, key: &Q) -> Option<(&K, &V)> {
        self.map.raw.get(key, &self.guard)
    }

    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<&V> {
        match self.map.raw.insert(key, value, true, &self.guard) {
            raw::InsertResult::Inserted(_) => None,
            raw::InsertResult::Replaced(old) => Some(old),
            raw::InsertResult::Error { .. } => unreachable!("replacement enabled"),
        }
    }

    #[inline]
    pub fn remove<Q: Equivalent<K> + Hash + ?Sized>(&self, key: &Q) -> Option<&V> {
        self.map
            .raw
            .remove(key, &self.guard)
            .map(|(_, value)| value)
    }

    /// Atomic read/modify/write. A racing expiration can retry the callback;
    /// callbacks must not perform external side effects.
    #[inline]
    pub fn compute<'g, F, T>(&'g self, key: K, compute: F) -> Compute<'g, K, V, T>
    where
        F: FnMut(Option<(&'g K, &'g V)>) -> Operation<V, T>,
    {
        self.map.raw.compute(key, compute, &self.guard)
    }

    #[inline]
    pub fn clear(&self) {
        self.map.raw.clear(&self.guard)
    }

    #[inline]
    pub fn retain(&self, keep: impl FnMut(&K, &V) -> bool) {
        self.map.raw.retain(keep, &self.guard)
    }

    /// Ordinary, duplicate-free iteration used by snapshot serialization.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.raw.iter(&self.guard)
    }

    /// Visit complete logical hash buckets. COUNT is a candidate/work hint,
    /// before caller filtering; zero starts/finishes a full iteration. Keys
    /// present throughout an iteration are covered despite concurrent writes
    /// and resizing. Duplicates and empty pages with nonzero cursors are valid.
    pub fn scan(&self, cursor: u64, count: usize, visit: impl FnMut(&K, &V)) -> u64 {
        self.map.raw.scan(cursor, count, visit, &self.guard)
    }
}
