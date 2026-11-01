//! An indexed skiplist of small sorted blocks.
//!
//! Spans count members, not blocks. Keeping nearby members together reduces
//! dependent memory accesses during searches and amortizes tower allocations.
//! Arena indices keep links valid across growth and cloning; slots are reused.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::iter::FusedIterator;
use std::mem::size_of;

const MAX_LEVEL: usize = 32;
const BLOCK_SIZE: usize = 64;
const NONE: usize = usize::MAX;
type Key = (OrderedFloat<f64>, Bytes);

// Scores have already been checked for NaN at the protocol/restore boundary.
#[inline]
fn compare_keys(left: &Key, right: &Key) -> Ordering {
    if left.0.0 < right.0.0 {
        Ordering::Less
    } else if left.0.0 > right.0.0 {
        Ordering::Greater
    } else {
        left.1.cmp(&right.1)
    }
}

#[derive(Clone, Copy, Debug)]
struct Link {
    next: usize,
    span: usize,
}
impl Link {
    const EMPTY: Self = Self {
        next: NONE,
        span: 0,
    };
}

#[derive(Debug)]
struct Node {
    keys: Vec<Key>,
    backward: usize,
    links: Box<[Link]>,
    max_score: f64,
}

impl Clone for Node {
    fn clone(&self) -> Self {
        // Keep cached allocation accounting correct when cloning snapshots.
        let mut keys = Vec::with_capacity(self.keys.capacity());
        keys.extend(self.keys.iter().cloned());
        Self {
            keys,
            backward: self.backward,
            links: self.links.clone(),
            max_score: self.max_score,
        }
    }
}

impl Node {
    fn new(keys: Vec<Key>, height: usize) -> Self {
        let max_score = keys.last().expect("nonempty skiplist block").0.0;
        Self {
            keys,
            backward: NONE,
            links: vec![Link::EMPTY; height].into_boxed_slice(),
            max_score,
        }
    }
    #[inline]
    fn refresh_max(&mut self) {
        self.max_score = self.keys.last().expect("nonempty skiplist block").0.0;
    }
    #[inline]
    fn ends_before(&self, key: &Key) -> bool {
        self.max_score < key.0.0
            || (self.max_score == key.0.0 && self.keys.last().unwrap().1 < key.1)
    }
}

#[derive(Clone, Debug)]
pub(super) struct IndexedSkipList {
    head: [Link; MAX_LEVEL],
    nodes: Vec<Option<Node>>,
    free: Vec<usize>,
    tail: usize,
    levels: usize,
    len: usize,
    allocated_links: usize,
    allocated_keys: usize,
}
impl Default for IndexedSkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexedSkipList {
    pub(super) fn new() -> Self {
        Self {
            head: [Link::EMPTY; MAX_LEVEL],
            nodes: Vec::new(),
            free: Vec::new(),
            tail: NONE,
            levels: 1,
            len: 0,
            allocated_links: 0,
            allocated_keys: 0,
        }
    }
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }
    #[inline]
    fn node(&self, index: usize) -> &Node {
        self.nodes[index].as_ref().expect("live skiplist block")
    }
    #[inline]
    fn node_mut(&mut self, index: usize) -> &mut Node {
        self.nodes[index].as_mut().expect("live skiplist block")
    }
    #[inline]
    fn link(&self, index: usize, level: usize) -> Link {
        if index == NONE {
            self.head[level]
        } else {
            self.node(index).links[level]
        }
    }
    #[inline]
    fn link_mut(&mut self, index: usize, level: usize) -> &mut Link {
        if index == NONE {
            &mut self.head[level]
        } else {
            &mut self.node_mut(index).links[level]
        }
    }
    fn random_height() -> usize {
        // P = 1/4. The random index layout never affects logical results.
        (rand::random::<u64>().trailing_zeros() as usize / 2 + 1).min(MAX_LEVEL)
    }

    /// Find predecessors of the first block ending at or after key. Member
    /// operations stop before the tail so appending can use its spare capacity.
    #[inline]
    fn path<const MEMBER: bool>(
        &self,
        key: &Key,
    ) -> ([usize; MAX_LEVEL], [usize; MAX_LEVEL], usize) {
        let mut update = [NONE; MAX_LEVEL];
        let mut ranks = [0; MAX_LEVEL];
        let mut current = NONE;
        let mut rank = 0;
        for level in (0..self.levels).rev() {
            loop {
                let link = self.link(current, level);
                if link.next == NONE
                    || (MEMBER && link.next == self.tail)
                    || !self.node(link.next).ends_before(key)
                {
                    break;
                }
                rank += link.span;
                current = link.next;
            }
            update[level] = current;
            ranks[level] = rank;
        }
        (update, ranks, self.link(current, 0).next)
    }

    /// Expected O(log N); block moves are bounded by BLOCK_SIZE.
    pub(super) fn insert(&mut self, key: Key) -> bool {
        debug_assert!(!key.0.0.is_nan());
        let (update, ranks, target) = self.path::<true>(&key);
        if target == NONE {
            let index = self.free.pop().unwrap_or(self.nodes.len());
            self.insert_node(
                index,
                Node::new(vec![key], Self::random_height()),
                &update,
                &ranks,
                NONE,
            );
            return true;
        }
        let position = match self
            .node(target)
            .keys
            .binary_search_by(|candidate| compare_keys(candidate, &key))
        {
            Ok(_) => return false,
            Err(position) => position,
        };
        let node = self.node_mut(target);
        let old_capacity = node.keys.capacity();
        if node.keys.len() == old_capacity {
            let capacity = (old_capacity * 2).min(BLOCK_SIZE + 1);
            node.keys.reserve_exact(capacity - old_capacity);
        }
        node.keys.insert(position, key);
        node.refresh_max();
        self.allocated_keys += node.keys.capacity() - old_capacity;
        self.len += 1;
        for (level, predecessor) in update.iter().enumerate().take(self.levels) {
            self.link_mut(*predecessor, level).span += 1;
        }
        if self.node(target).keys.len() > BLOCK_SIZE {
            self.split_node(target, &update, &ranks);
        }
        true
    }

    fn insert_node(
        &mut self,
        index: usize,
        mut node: Node,
        update: &[usize; MAX_LEVEL],
        ranks: &[usize; MAX_LEVEL],
        next: usize,
    ) {
        let height = node.links.len();
        let weight = node.keys.len();
        for level in self.levels..height {
            self.head[level].span = self.len;
        }
        self.levels = self.levels.max(height);
        node.backward = update[0];
        for level in 0..height {
            let predecessor = self.link_mut(update[level], level);
            let distance = ranks[0] - ranks[level];
            node.links[level] = Link {
                next: predecessor.next,
                span: predecessor.span - distance,
            };
            *predecessor = Link {
                next: index,
                span: distance + weight,
            };
        }
        for (level, predecessor) in update.iter().enumerate().take(self.levels).skip(height) {
            self.link_mut(*predecessor, level).span += weight;
        }
        if next == NONE {
            self.tail = index;
        } else {
            self.node_mut(next).backward = index;
        }
        self.allocated_links += height;
        self.allocated_keys += node.keys.capacity();
        if index == self.nodes.len() {
            self.nodes.push(Some(node));
        } else {
            self.nodes[index] = Some(node);
        }
        self.len += weight;
    }

    fn split_node(
        &mut self,
        index: usize,
        update: &[usize; MAX_LEVEL],
        ranks: &[usize; MAX_LEVEL],
    ) {
        let mut left = self.unlink_node(index, update);
        let mut right_keys = Vec::with_capacity(BLOCK_SIZE + 1);
        right_keys.extend(left.keys.drain(BLOCK_SIZE / 2..));
        left.refresh_max();
        let next = self.link(update[0], 0).next;
        self.insert_node(index, left, update, ranks, next);
        let right = Node::new(right_keys, Self::random_height());
        let (update, ranks, next) = self.path::<false>(&right.keys[0]);
        let index = self.free.pop().unwrap_or(self.nodes.len());
        self.insert_node(index, right, &update, &ranks, next);
    }

    pub(super) fn remove(&mut self, key: &Key) -> bool {
        let (update, _, target) = self.path::<true>(key);
        if target == NONE {
            return false;
        }
        let Ok(position) = self
            .node(target)
            .keys
            .binary_search_by(|candidate| compare_keys(candidate, key))
        else {
            return false;
        };
        self.remove_at(target, position, &update);
        true
    }

    fn remove_at(&mut self, index: usize, position: usize, update: &[usize; MAX_LEVEL]) -> Key {
        if self.node(index).keys.len() == 1 {
            let mut node = self.unlink_node(index, update);
            self.free.push(index);
            return node.keys.pop().unwrap();
        }
        let node = self.node_mut(index);
        let key = node.keys.remove(position);
        node.refresh_max();
        self.len -= 1;
        for (level, predecessor) in update.iter().enumerate().take(self.levels) {
            self.link_mut(*predecessor, level).span -= 1;
        }
        self.maybe_merge(index, update);
        key
    }

    fn maybe_merge(&mut self, index: usize, update: &[usize; MAX_LEVEL]) {
        let node = self.node(index);
        let next = node.links[0].next;
        if node.keys.len() >= BLOCK_SIZE / 2
            || next == NONE
            || node.keys.len() + self.node(next).keys.len() > BLOCK_SIZE
        {
            return;
        }
        // These blocks are adjacent. Only levels present on the left block
        // acquire a new predecessor; every higher predecessor is unchanged.
        let mut right_update = *update;
        right_update[..node.links.len()].fill(index);
        let mut right = self.unlink_node(next, &right_update);
        let weight = right.keys.len();
        let node = self.node_mut(index);
        let old_capacity = node.keys.capacity();
        node.keys.append(&mut right.keys);
        node.refresh_max();
        self.allocated_keys += node.keys.capacity() - old_capacity;
        self.len += weight;
        for (level, predecessor) in update.iter().enumerate().take(self.levels) {
            self.link_mut(*predecessor, level).span += weight;
        }
        self.free.push(next);
    }

    /// Updates inside a block without changing spans. Cross-block moves use
    /// the existing removal path, then locate their destination.
    pub(super) fn update_score(&mut self, old_key: &Key, new_score: f64) -> bool {
        debug_assert!(!new_score.is_nan());
        let (update, _, target) = self.path::<true>(old_key);
        if target == NONE {
            return false;
        }
        let Ok(position) = self
            .node(target)
            .keys
            .binary_search_by(|candidate| compare_keys(candidate, old_key))
        else {
            return false;
        };
        let node = self.node(target);
        let previous = node.backward;
        let next = node.links[0].next;
        let new_score = OrderedFloat(new_score);
        let compare = |candidate: &Key| {
            if candidate.0.0 < new_score.0 {
                Ordering::Less
            } else if candidate.0.0 > new_score.0 {
                Ordering::Greater
            } else {
                candidate.1.cmp(&old_key.1)
            }
        };
        let before = if position > 0 {
            Some(&node.keys[position - 1])
        } else if previous != NONE {
            self.node(previous).keys.last()
        } else {
            None
        };
        let after = if position + 1 < node.keys.len() {
            Some(&node.keys[position + 1])
        } else if next != NONE {
            Some(&self.node(next).keys[0])
        } else {
            None
        };
        if before.is_none_or(|key| compare(key).is_lt())
            && after.is_none_or(|key| compare(key).is_gt())
        {
            let node = self.node_mut(target);
            node.keys[position].0 = new_score;
            if position + 1 == node.keys.len() {
                node.max_score = new_score.0;
            }
            return true;
        }
        let stays_in_block = (previous == NONE
            || compare(self.node(previous).keys.last().unwrap()).is_lt())
            && (next == NONE || compare(&self.node(next).keys[0]).is_gt());
        if stays_in_block {
            let node = self.node_mut(target);
            let mut key = node.keys.remove(position);
            key.0 = new_score;
            let position = node
                .keys
                .binary_search_by(|candidate| compare_keys(candidate, &key))
                .unwrap_err();
            node.keys.insert(position, key);
            node.refresh_max();
        } else {
            let mut key = self.remove_at(target, position, &update);
            key.0 = new_score;
            self.insert(key);
        }
        true
    }

    fn unlink_node(&mut self, index: usize, update: &[usize; MAX_LEVEL]) -> Node {
        let node = self.nodes[index].take().expect("live skiplist block");
        let weight = node.keys.len();
        for (level, predecessor) in update.iter().enumerate().take(self.levels) {
            let link = self.link_mut(*predecessor, level);
            if link.next == index {
                link.span += node.links[level].span;
                link.next = node.links[level].next;
            }
            link.span -= weight;
        }
        let next = node.links[0].next;
        if next == NONE {
            self.tail = node.backward;
        } else {
            self.node_mut(next).backward = node.backward;
        }
        while self.levels > 1 && self.head[self.levels - 1].next == NONE {
            self.levels -= 1;
        }
        self.len -= weight;
        self.allocated_links -= node.links.len();
        self.allocated_keys -= node.keys.capacity();
        node
    }

    /// Counts a monotonic true prefix in expected O(log N).
    pub(super) fn partition_point<F>(&self, predicate: F) -> usize
    where
        F: FnMut(&Key) -> bool,
    {
        self.prefix_position(predicate).2
    }

    fn prefix_position<F>(&self, mut predicate: F) -> (usize, usize, usize)
    where
        F: FnMut(&Key) -> bool,
    {
        let mut current = NONE;
        let mut rank = 0;
        for level in (0..self.levels).rev() {
            loop {
                let link = self.link(current, level);
                if link.next == NONE || !predicate(self.node(link.next).keys.last().unwrap()) {
                    break;
                }
                rank += link.span;
                current = link.next;
            }
        }
        let next = self.link(current, 0).next;
        let offset = if next == NONE {
            0
        } else {
            self.node(next).keys.partition_point(predicate)
        };
        (next, offset, rank + offset)
    }

    /// Iterates after a monotonic prefix with only one boundary search.
    pub(super) fn iter_from_prefix<F>(&self, predicate: F) -> Iter<'_>
    where
        F: FnMut(&Key) -> bool,
    {
        let (front, front_offset, rank) = self.prefix_position(predicate);
        Iter {
            list: self,
            front,
            front_offset,
            front_rank: rank,
            back: self.tail,
            back_offset: if self.tail == NONE {
                0
            } else {
                self.node(self.tail).keys.len() - 1
            },
            remaining: self.len - rank,
        }
    }

    /// Iterates a monotonic prefix; reverse iteration starts at its boundary.
    pub(super) fn iter_to_prefix<F>(&self, predicate: F) -> Iter<'_>
    where
        F: FnMut(&Key) -> bool,
    {
        let (node, offset, rank) = self.prefix_position(predicate);
        let (back, back_offset) = if rank == 0 {
            (NONE, 0)
        } else if node == NONE {
            (self.tail, self.node(self.tail).keys.len() - 1)
        } else if offset == 0 {
            let previous = self.node(node).backward;
            (previous, self.node(previous).keys.len() - 1)
        } else {
            (node, offset - 1)
        };
        Iter {
            list: self,
            front: self.head[0].next,
            front_offset: 0,
            front_rank: 0,
            back,
            back_offset,
            remaining: rank,
        }
    }

    pub(super) fn rank(&self, key: &Key) -> Option<usize> {
        let (_, ranks, target) = self.path::<true>(key);
        if target == NONE {
            return None;
        }
        self.node(target)
            .keys
            .binary_search_by(|candidate| compare_keys(candidate, key))
            .ok()
            .map(|position| ranks[0] + position)
    }

    fn position_at_rank(&self, rank: usize) -> (usize, usize) {
        if rank >= self.len {
            return (NONE, 0);
        }
        let mut current = NONE;
        let mut traversed = 0;
        for level in (0..self.levels).rev() {
            loop {
                let link = self.link(current, level);
                if link.next == NONE || link.span > rank - traversed {
                    break;
                }
                traversed += link.span;
                current = link.next;
            }
        }
        (self.link(current, 0).next, rank - traversed)
    }

    pub(super) fn iter(&self) -> Iter<'_> {
        Iter {
            list: self,
            front: self.head[0].next,
            front_offset: 0,
            front_rank: 0,
            back: self.tail,
            back_offset: if self.tail == NONE {
                0
            } else {
                self.node(self.tail).keys.len() - 1
            },
            remaining: self.len,
        }
    }
    pub(super) fn iter_range(&self, start: usize, end: usize) -> Iter<'_> {
        let end = end.min(self.len);
        if start >= end {
            return Iter {
                list: self,
                front: NONE,
                front_offset: 0,
                front_rank: 0,
                back: NONE,
                back_offset: 0,
                remaining: 0,
            };
        }
        let (front, front_offset) = if start == 0 {
            (self.head[0].next, 0)
        } else {
            self.position_at_rank(start)
        };
        let (back, back_offset) = if end == self.len {
            (self.tail, self.node(self.tail).keys.len() - 1)
        } else {
            self.position_at_rank(end - 1)
        };
        Iter {
            list: self,
            front,
            front_offset,
            front_rank: start,
            back,
            back_offset,
            remaining: end - start,
        }
    }
    pub(super) fn pop_first(&mut self) -> Option<Key> {
        let first = self.head[0].next;
        (first != NONE).then(|| self.remove_at(first, 0, &[NONE; MAX_LEVEL]))
    }
    pub(super) fn allocated_bytes(&self) -> usize {
        self.nodes
            .capacity()
            .saturating_mul(size_of::<Option<Node>>())
            .saturating_add(self.free.capacity().saturating_mul(size_of::<usize>()))
            .saturating_add(self.allocated_links.saturating_mul(size_of::<Link>()))
            .saturating_add(self.allocated_keys.saturating_mul(size_of::<Key>()))
    }
}

pub(super) struct Iter<'a> {
    list: &'a IndexedSkipList,
    front: usize,
    front_offset: usize,
    front_rank: usize,
    back: usize,
    back_offset: usize,
    remaining: usize,
}
impl<'a> Iterator for Iter<'a> {
    type Item = &'a Key;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let node = self.list.node(self.front);
        let key = &node.keys[self.front_offset];
        self.front_offset += 1;
        if self.front_offset == node.keys.len() {
            self.front = node.links[0].next;
            self.front_offset = 0;
        }
        self.remaining -= 1;
        self.front_rank += 1;
        Some(key)
    }
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.remaining {
            self.remaining = 0;
            return None;
        }
        if n != 0 {
            self.front_rank += n;
            (self.front, self.front_offset) = self.list.position_at_rank(self.front_rank);
            self.remaining -= n;
        }
        self.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl DoubleEndedIterator for Iter<'_> {
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.remaining {
            self.remaining = 0;
            return None;
        }
        if n != 0 {
            let rank = self.front_rank + self.remaining - 1 - n;
            (self.back, self.back_offset) = self.list.position_at_rank(rank);
            self.remaining -= n;
        }
        self.next_back()
    }
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let node = self.list.node(self.back);
        let key = &node.keys[self.back_offset];
        if self.back_offset == 0 {
            self.back = node.backward;
            self.back_offset = if self.back == NONE {
                0
            } else {
                self.list.node(self.back).keys.len() - 1
            };
        } else {
            self.back_offset -= 1;
        }
        self.remaining -= 1;
        Some(key)
    }
}
impl ExactSizeIterator for Iter<'_> {}
impl FusedIterator for Iter<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::BTreeSet;

    fn key(value: u32) -> Key {
        (
            OrderedFloat((value % 37) as f64),
            Bytes::from(value.to_be_bytes().to_vec()),
        )
    }

    fn assert_invariants(list: &IndexedSkipList) {
        let mut ranks = vec![None; list.nodes.len()];
        let mut current = list.head[0].next;
        let mut previous = NONE;
        let mut length = 0;
        let mut blocks = 0;
        let mut allocated_links = 0;
        let mut allocated_keys = 0;
        while current != NONE {
            let node = list.node(current);
            assert_eq!(node.backward, previous);
            assert!(!node.keys.is_empty());
            assert!(node.keys.len() <= BLOCK_SIZE);
            assert!(node.keys.windows(2).all(|pair| pair[0] < pair[1]));
            assert_eq!(node.max_score, node.keys.last().unwrap().0.0);
            if previous != NONE {
                assert!(list.node(previous).keys.last().unwrap() < &node.keys[0]);
            }
            length += node.keys.len();
            blocks += 1;
            ranks[current] = Some(length);
            allocated_links += node.links.len();
            allocated_keys += node.keys.capacity();
            previous = current;
            current = node.links[0].next;
        }
        assert_eq!(length, list.len);
        assert_eq!(previous, list.tail);
        assert_eq!(allocated_links, list.allocated_links);
        assert_eq!(allocated_keys, list.allocated_keys);
        assert_eq!(list.nodes.len(), blocks + list.free.len());
        let mut free = list.free.clone();
        free.sort_unstable();
        free.dedup();
        assert_eq!(free.len(), list.free.len());
        for &index in &free {
            assert!(list.nodes[index].is_none());
        }
        for level in 0..list.levels {
            let mut index = NONE;
            let mut rank = 0;
            loop {
                let link = list.link(index, level);
                if link.next == NONE {
                    assert_eq!(link.span, list.len - rank);
                    break;
                }
                let next_rank = ranks[link.next].expect("linked block is live");
                assert_eq!(link.span, next_rank - rank);
                assert!(list.node(link.next).links.len() > level);
                index = link.next;
                rank = next_rank;
            }
        }
    }

    #[test]
    fn randomized_insert_remove_rank_and_bounds_match_btree() {
        let mut list = IndexedSkipList::new();
        let mut expected = BTreeSet::new();
        let mut rng = StdRng::seed_from_u64(0x5c0e_71a9);
        for step in 0..20_000 {
            let key = key(rng.gen_range(0..2048));
            if rng.gen_bool(0.55) {
                assert_eq!(list.insert(key.clone()), expected.insert(key));
            } else {
                assert_eq!(list.remove(&key), expected.remove(&key));
            }
            if step % 97 == 0 {
                assert_invariants(&list);
                assert_eq!(
                    list.iter().collect::<Vec<_>>(),
                    expected.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    list.iter().rev().collect::<Vec<_>>(),
                    expected.iter().rev().collect::<Vec<_>>()
                );
                for (rank, key) in expected.iter().enumerate() {
                    assert_eq!(list.rank(key), Some(rank));
                }
                for score in [-1.0, 0.0, 18.0, 36.0, 37.0] {
                    assert_eq!(
                        list.partition_point(|key| key.0.0 < score),
                        expected.iter().take_while(|key| key.0.0 < score).count()
                    );
                    assert_eq!(
                        list.partition_point(|key| key.0.0 <= score),
                        expected.iter().take_while(|key| key.0.0 <= score).count()
                    );
                }
            }
        }
        assert_invariants(&list);
        assert_eq!(list.rank(&key(3000)), None);
        assert_eq!(list.partition_point(|_| true), list.len());
        assert_eq!(list.partition_point(|_| false), 0);
    }

    #[test]
    fn rank_ranges_support_interleaved_double_ended_iteration() {
        let mut list = IndexedSkipList::new();
        for value in (0..300).rev() {
            list.insert(key(value));
        }
        let expected: Vec<_> = list.iter().cloned().collect();
        for (start, end) in [
            (0, 300),
            (100, 110),
            (299, 300),
            (0, 1),
            (0, 0),
            (300, 400),
            (7, 3),
            (usize::MAX, usize::MAX),
        ] {
            let mut iter = list.iter_range(start, end);
            let end = end.min(expected.len());
            let slice = if start < end {
                &expected[start..end]
            } else {
                &[]
            };
            let mut expected_iter = slice.iter();
            loop {
                assert_eq!(iter.len(), expected_iter.len());
                assert_eq!(iter.next(), expected_iter.next());
                assert_eq!(iter.next_back(), expected_iter.next_back());
                if expected_iter.len() == 0 {
                    assert_eq!(iter.next(), None);
                    assert_eq!(iter.next_back(), None);
                    break;
                }
            }
        }
    }

    #[test]
    fn splits_merges_pop_and_clone_keep_member_spans_and_reuse_slots() {
        let mut list = IndexedSkipList::new();
        assert_eq!(list.allocated_bytes(), 0);
        for value in 0..1024 {
            list.insert(key(value));
        }
        let original = list.clone();
        let original_keys: Vec<_> = original.iter().cloned().collect();
        let slots = list.nodes.len();
        for key in original_keys.iter().step_by(2) {
            assert!(list.remove(key));
            assert_invariants(&list);
        }
        for key in list.iter().cloned().collect::<Vec<_>>() {
            assert_eq!(list.pop_first(), Some(key));
            assert_invariants(&list);
        }
        assert!(list.is_empty());
        assert_eq!(list.pop_first(), None);
        assert_eq!(original.len(), 1024);
        assert_invariants(&original);
        for key in &original_keys[..BLOCK_SIZE] {
            list.insert(key.clone());
        }
        assert_eq!(list.nodes.len(), slots);
        assert_invariants(&list);
        assert_eq!(original.iter().cloned().collect::<Vec<_>>(), original_keys);
        assert_invariants(&list.clone());
        let allocated = list.nodes.capacity() * size_of::<Option<Node>>()
            + list.free.capacity() * size_of::<usize>()
            + list
                .nodes
                .iter()
                .flatten()
                .map(|node| {
                    node.links.len() * size_of::<Link>() + node.keys.capacity() * size_of::<Key>()
                })
                .sum::<usize>();
        assert_eq!(list.allocated_bytes(), allocated);
    }

    #[test]
    fn score_updates_within_and_across_blocks_preserve_clone_and_ties() {
        let mut list = IndexedSkipList::new();
        for value in 0..256 {
            list.insert(key(value));
        }
        let original = list.clone();
        let original_keys: Vec<_> = original.iter().cloned().collect();
        let mut expected: BTreeSet<_> = original_keys.iter().cloned().collect();
        let mut old = key(10);
        for score in [10.25, 4.0, 1.0, 0.0, f64::INFINITY, f64::NEG_INFINITY, 2.0] {
            assert!(list.update_score(&old, score));
            expected.remove(&old);
            old.0 = OrderedFloat(score);
            expected.insert(old.clone());
            assert_eq!(
                list.iter().collect::<Vec<_>>(),
                expected.iter().collect::<Vec<_>>()
            );
            for (rank, key) in expected.iter().enumerate() {
                assert_eq!(list.rank(key), Some(rank));
            }
            assert_invariants(&list);
        }
        assert!(!list.update_score(&(OrderedFloat(2.0), Bytes::from_static(b"missing")), 10.0));
        assert_eq!(original.iter().cloned().collect::<Vec<_>>(), original_keys);
        assert_invariants(&original);
    }

    #[test]
    fn prefix_ranges_and_rank_skips_match_slice_iterators() {
        let mut list = IndexedSkipList::new();
        for value in (0..5000).rev() {
            list.insert(key(value));
        }
        let keys: Vec<_> = list.iter().cloned().collect();
        for bound in [-1.0, 0.0, 3.0, 18.0, 36.0, 37.0] {
            let rank = keys.partition_point(|key| key.0.0 < bound);
            assert_eq!(
                list.iter_from_prefix(|key| key.0.0 < bound)
                    .collect::<Vec<_>>(),
                keys[rank..].iter().collect::<Vec<_>>()
            );
            assert_eq!(
                list.iter_to_prefix(|key| key.0.0 < bound)
                    .rev()
                    .collect::<Vec<_>>(),
                keys[..rank].iter().rev().collect::<Vec<_>>()
            );
        }
        for (start, end) in [(0, 5000), (31, 4097), (3200, 3201)] {
            let mut actual = list.iter_range(start, end);
            let mut expected = keys[start..end].iter();
            for n in [0, 1, 97, 13, 0, 411, 1024, usize::MAX] {
                assert_eq!(actual.nth(n), expected.nth(n));
                assert_eq!(actual.nth_back(n), expected.nth_back(n));
                assert_eq!(actual.len(), expected.len());
            }
            assert_eq!(actual.next(), expected.next());
            assert_eq!(actual.next_back(), expected.next_back());
        }
        let empty = IndexedSkipList::new();
        assert_eq!(empty.iter_from_prefix(|_| true).next(), None);
        assert_eq!(empty.iter_to_prefix(|_| true).next_back(), None);
    }

    #[test]
    fn very_tall_blocks_keep_spans_when_split_merged_and_removed() {
        let mut list = IndexedSkipList::new();
        for (block, height) in [1, MAX_LEVEL, 4, 1, 16, 3, 8, 2].into_iter().enumerate() {
            let keys: Vec<_> = (block * 16..block * 16 + 16)
                .map(|value| (OrderedFloat(value as f64), Bytes::from(value.to_string())))
                .collect();
            let (update, ranks, next) = list.path::<false>(&keys[0]);
            list.insert_node(
                list.nodes.len(),
                Node::new(keys, height),
                &update,
                &ranks,
                next,
            );
        }
        assert_eq!(list.levels, MAX_LEVEL);
        assert_invariants(&list);
        // Remove alternating members so merging can eliminate a maximum tower.
        let keys: Vec<_> = list.iter().cloned().collect();
        for key in keys.iter().step_by(2).chain(keys.iter().skip(1).step_by(2)) {
            assert!(list.remove(key));
            assert_invariants(&list);
            for (rank, key) in list.iter().enumerate() {
                assert_eq!(list.rank(key), Some(rank));
            }
        }
        assert_eq!(list.levels, 1);
        assert!(list.is_empty());
    }

    #[test]
    fn randomized_score_changes_preserve_spans_and_tie_order() {
        let mut list = IndexedSkipList::new();
        let mut keys: Vec<_> = (0..256).map(key).collect();
        let mut expected: BTreeSet<_> = keys.iter().cloned().collect();
        for key in &keys {
            list.insert(key.clone());
        }
        let mut rng = StdRng::seed_from_u64(0xb0a7_19e3);
        for step in 0..5_000 {
            let index = rng.gen_range(0..keys.len());
            let key = &mut keys[index];
            let score = if step % 97 == 0 {
                f64::INFINITY
            } else if step % 113 == 0 {
                f64::NEG_INFINITY
            } else {
                rng.gen_range(-32..32) as f64
            };
            assert!(list.update_score(key, score));
            expected.remove(key);
            key.0 = OrderedFloat(score);
            expected.insert(key.clone());
            if step % 17 == 0 {
                assert_invariants(&list);
                assert_eq!(
                    list.iter().collect::<Vec<_>>(),
                    expected.iter().collect::<Vec<_>>()
                );
                for (rank, key) in expected.iter().enumerate() {
                    assert_eq!(list.rank(key), Some(rank));
                }
            }
        }
        assert_invariants(&list);
    }

    #[test]
    fn ties_binary_members_infinities_and_signed_zero_use_tuple_order() {
        let mut list = IndexedSkipList::new();
        let keys = [
            (OrderedFloat(f64::INFINITY), Bytes::from_static(b"z")),
            (OrderedFloat(0.0), Bytes::new()),
            (OrderedFloat(-0.0), Bytes::from_static(b"a\0")),
            (OrderedFloat(0.0), Bytes::from_static(b"a")),
            (OrderedFloat(f64::NEG_INFINITY), Bytes::from_static(b"\xff")),
        ];
        for key in &keys {
            assert!(list.insert(key.clone()));
            assert!(!list.insert(key.clone()));
        }
        assert!(!list.insert((OrderedFloat(-0.0), Bytes::new())));
        let expected: BTreeSet<_> = keys.into_iter().collect();
        assert_eq!(
            list.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_invariants(&list);
        for key in expected {
            assert!(list.remove(&key));
            assert_invariants(&list);
        }
    }
}
