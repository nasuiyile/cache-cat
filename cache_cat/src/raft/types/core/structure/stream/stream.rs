use blart::TreeMap;
use std::{collections::HashMap, fmt, ops::Bound, sync::Arc};

use crate::raft::types::core::structure::stream::types::*;
type Key = [u8; 16];

#[derive(Clone, Debug)]
struct Pending {
    consumer: Vec<u8>,
    delivery_ms: u64,
    deliveries: u64,
}

#[derive(Clone, Debug)]
struct Consumer {
    pel: TreeMap<Key, ()>,
    seen_ms: u64,
    active_ms: Option<u64>,
}
impl Consumer {
    fn new(now: u64) -> Self {
        Self {
            pel: TreeMap::new(),
            seen_ms: now,
            active_ms: None,
        }
    }
}

#[derive(Clone, Debug)]
struct Group {
    last_delivered: StreamId,
    entries_read: Option<u64>,
    pel: TreeMap<Key, Pending>,
    consumers: HashMap<Vec<u8>, Consumer>,
    /// Detect destroy/recreate while a SharedStream reader is sleeping.
    identity: Arc<()>,
}
impl Group {
    fn new(last_delivered: StreamId, entries_read: Option<u64>) -> Self {
        Self {
            last_delivered,
            entries_read,
            pel: TreeMap::new(),
            consumers: HashMap::new(),
            identity: Arc::new(()),
        }
    }

    fn touch(&mut self, consumer: &[u8], now: u64) {
        let c = self
            .consumers
            .entry(consumer.to_vec())
            .or_insert_with(|| Consumer::new(now));
        c.seen_ms = now;
    }

    fn active(&mut self, consumer: &[u8], now: u64) {
        self.consumers
            .get_mut(consumer)
            .expect("consumer was created")
            .active_ms = Some(now);
    }

    fn remove_pending(&mut self, key: &Key) -> Option<Pending> {
        let pending = self.pel.remove(key)?;
        if let Some(c) = self.consumers.get_mut(pending.consumer.as_slice()) {
            let _ = c.pel.remove(key);
        }
        Some(pending)
    }

    /// The group PEL owns metadata; the consumer PEL is only a secondary index.
    fn assign(&mut self, key: Key, pending: Pending) {
        let _ = self.remove_pending(&key);
        let c = self
            .consumers
            .get_mut(pending.consumer.as_slice())
            .expect("consumer was created");
        let _ = c.pel.insert(key, ());
        let _ = self.pel.insert(key, pending);
    }

    fn advance(&mut self, id: StreamId, meta: Meta) {
        if id <= self.last_delivered {
            return;
        }
        self.entries_read = match self.entries_read {
            Some(read)
                if self.last_delivered >= meta.first
                    && !meta.tombstones_from(self.last_delivered) =>
            {
                read.checked_add(1)
            }
            _ => meta.estimate(id),
        };
        self.last_delivered = id;
    }
}

#[derive(Clone, Copy)]
struct Meta {
    len: u64,
    first: StreamId,
    last: StreamId,
    added: u64,
    max_deleted: StreamId,
}
impl Meta {
    fn tombstones_from(self, id: StreamId) -> bool {
        self.len != 0 && self.max_deleted != StreamId::ZERO && id <= self.max_deleted
    }

    /// Infer a logical counter only where the retained metadata permits it.
    fn estimate(self, id: StreamId) -> Option<u64> {
        if self.added == 0 {
            return Some(0);
        }
        if self.len == 0 && id <= self.last {
            return Some(self.added);
        }
        if id != StreamId::ZERO && id < self.max_deleted {
            return None;
        }
        if id == self.last {
            return Some(self.added);
        }
        if id > self.last {
            return None;
        }
        if self.max_deleted == StreamId::ZERO || self.max_deleted < self.first {
            if id < self.first {
                return self.added.checked_sub(self.len);
            }
            if id == self.first {
                return self.added.checked_sub(self.len)?.checked_add(1);
            }
        }
        None
    }

    fn lag(self, group: &Group) -> Option<u64> {
        if self.added == 0 || self.len == 0 {
            return Some(0);
        }
        if group.last_delivered < self.first && self.max_deleted < self.first {
            return Some(self.len);
        }
        if !self.tombstones_from(group.last_delivered) {
            if let Some(read) = group.entries_read {
                return self.added.checked_sub(read);
            }
        }
        self.added.checked_sub(self.estimate(group.last_delivered)?)
    }
}

fn keys_in_range<V>(map: &TreeMap<Key, V>, range: IdRange, count: usize) -> Vec<Key> {
    let Some((lo, hi)) = range.normalized() else {
        return Vec::new();
    };
    map.range::<Key, _>((Bound::Included(lo.to_key()), Bound::Included(hi.to_key())))
        .take(count)
        .map(|(key, _)| *key)
        .collect()
}

fn pending_info(key: &Key, p: &Pending, now: u64) -> PendingInfo {
    PendingInfo {
        id: StreamId::from_key(*key),
        consumer: p.consumer.clone(),
        idle_ms: now.saturating_sub(p.delivery_ms),
        deliveries: p.deliveries,
        last_delivery_ms: p.delivery_ms,
    }
}

/// One in-memory Stream value. All state is private; mutations require `&mut self`.
///
/// The main index and both PEL indexes use blart. Group/consumer names use
/// HashMap so that arbitrary binary names, including prefix-related names, work.
/// No network, persistence, keyspace, or allocator-specific encoding is implied.
#[derive(Clone)]
pub struct RedisStream {
    entries: TreeMap<Key, Fields>,
    groups: HashMap<Vec<u8>, Group>,
    last_generated_id: StreamId,
    entries_added: u64,
    max_deleted_entry_id: StreamId,
    clock: Arc<dyn Clock>,
}

impl fmt::Debug for RedisStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisStream")
            .field("length", &self.entries.len())
            .field("groups", &self.groups.len())
            .field("last_generated_id", &self.last_generated_id)
            .field("entries_added", &self.entries_added)
            .finish_non_exhaustive()
    }
}
impl Default for RedisStream {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisStream {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            entries: TreeMap::new(),
            groups: HashMap::new(),
            last_generated_id: StreamId::ZERO,
            entries_added: 0,
            max_deleted_entry_id: StreamId::ZERO,
            clock,
        }
    }

    pub fn xlen(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn last_generated_id(&self) -> StreamId {
        self.last_generated_id
    }
    pub fn contains(&self, id: StreamId) -> bool {
        self.entries.contains_key(&id.to_key())
    }

    pub fn get(&self, id: StreamId) -> Option<Entry> {
        self.entries.get(&id.to_key()).map(|fields| Entry {
            id,
            fields: fields.clone(),
        })
    }

    fn meta(&self) -> Meta {
        Meta {
            len: self.entries.len() as u64,
            first: self
                .entries
                .first_key_value()
                .map(|(k, _)| StreamId::from_key(*k))
                .unwrap_or(StreamId::ZERO),
            last: self.last_generated_id,
            added: self.entries_added,
            max_deleted: self.max_deleted_entry_id,
        }
    }

    fn resolve_group_start(&self, start: GroupStart) -> StreamId {
        match start {
            GroupStart::Id(id) => id,
            GroupStart::Last => self.last_generated_id,
        }
    }

    pub(crate) fn group_identity(&self, name: &[u8]) -> Result<Arc<()>> {
        Ok(self
            .groups
            .get(name)
            .ok_or(StreamError::NoGroup)?
            .identity
            .clone())
    }

    /// Cheap predicate for the async adapter's group wakeup handoff.
    pub(crate) fn group_has_new(&self, name: &[u8]) -> Result<bool> {
        let group = self.groups.get(name).ok_or(StreamError::NoGroup)?;
        Ok(self
            .entries
            .last_key_value()
            .is_some_and(|(key, _)| StreamId::from_key(*key) > group.last_delivered))
    }

    pub fn xadd(&mut self, id: AddId, fields: Fields) -> Result<StreamId> {
        self.xadd_with_options(id, fields, AddOptions::default())
    }

    /// Validate all fallible input/state checks before changing stream state.
    pub fn xadd_with_options(
        &mut self,
        request: AddId,
        fields: Fields,
        options: AddOptions,
    ) -> Result<StreamId> {
        if fields.is_empty() {
            return Err(StreamError::EmptyFields);
        }
        let last = self.last_generated_id;
        let id = match request {
            AddId::Explicit(id) => id,
            AddId::Auto => {
                let now = self.clock.now_ms();
                if now > last.ms {
                    StreamId::new(now, 0)
                } else {
                    last.successor().ok_or(StreamError::IdExhausted)?
                }
            }
            AddId::AutoSequence(ms) => {
                if ms < last.ms {
                    return Err(StreamError::IdNotIncreasing);
                }
                let seq = if ms == last.ms {
                    last.seq.checked_add(1).ok_or(StreamError::IdExhausted)?
                } else {
                    0
                };
                StreamId::new(ms, seq)
            }
        };
        if id == StreamId::ZERO {
            return Err(StreamError::ZeroId);
        }
        if id <= last {
            return Err(StreamError::IdNotIncreasing);
        }
        let added = self
            .entries_added
            .checked_add(1)
            .ok_or(StreamError::CounterOverflow)?;
        let _ = self.entries.insert(id.to_key(), fields);
        self.last_generated_id = id;
        self.entries_added = added;
        if let Some(trim) = options.trim {
            self.xtrim(trim);
        }
        Ok(id)
    }

    /// Ascending range. Some(0) returns an empty result; None has no limit.
    pub fn xrange(&self, range: IdRange, count: Option<usize>) -> Vec<Entry> {
        self.range_impl(range, count, false)
    }

    /// Descending range. `range.start` is still the LOW bound, unlike RESP syntax.
    pub fn xrevrange(&self, range: IdRange, count: Option<usize>) -> Vec<Entry> {
        self.range_impl(range, count, true)
    }

    /// Text adapter preserving the Redis XREVRANGE argument order: end, start.
    pub fn xrevrange_text(
        &self,
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<Entry>> {
        Ok(self.xrevrange(IdRange::parse(start, end)?, count))
    }

    fn range_impl(&self, range: IdRange, count: Option<usize>, reverse: bool) -> Vec<Entry> {
        let Some((lo, hi)) = range.normalized() else {
            return Vec::new();
        };
        let iter = self
            .entries
            .range::<Key, _>((Bound::Included(lo.to_key()), Bound::Included(hi.to_key())));
        let convert = |(key, fields): (&Key, &Fields)| Entry {
            id: StreamId::from_key(*key),
            fields: fields.clone(),
        };
        let limit = count.unwrap_or(usize::MAX);
        if reverse {
            iter.rev().take(limit).map(convert).collect()
        } else {
            iter.take(limit).map(convert).collect()
        }
    }

    /// Non-blocking XREAD. The cursor is exclusive and is never stored globally.
    pub fn xread(&self, after: StreamId, count: Option<usize>) -> Vec<Entry> {
        self.xrange(IdRange::after(after), count)
    }

    /// Non-blocking handling of `$` / `+`. `$` has no existing data to return.
    pub fn xread_from(&self, start: ReadStart, count: Option<usize>) -> Vec<Entry> {
        match start {
            ReadStart::After(id) => self.xread(id, count),
            ReadStart::Tail => Vec::new(),
            ReadStart::Latest => self.xrevrange(IdRange::all(), Some(1)),
        }
    }

    fn delete_payload(&mut self, id: StreamId, tombstone: bool) -> bool {
        if self.entries.remove(&id.to_key()).is_none() {
            return false;
        }
        if tombstone {
            self.max_deleted_entry_id = self.max_deleted_entry_id.max(id);
        }
        true
    }

    fn clear_references(&mut self, id: StreamId) {
        let key = id.to_key();
        for group in self.groups.values_mut() {
            let _ = group.remove_pending(&key);
        }
    }

    fn is_referenced(&self, id: StreamId) -> bool {
        let key = id.to_key();
        self.groups
            .values()
            .any(|g| g.last_delivered < id || g.pel.contains_key(&key))
    }

    /// Delete payloads only. PEL records, group cursors and last-generated ID survive.
    pub fn xdel(&mut self, ids: &[StreamId]) -> usize {
        ids.iter()
            .filter(|&&id| self.delete_payload(id, true))
            .count()
    }

    pub fn xdelex(&mut self, ids: &[StreamId], policy: ReferencePolicy) -> Vec<DeleteStatus> {
        ids.iter()
            .map(|&id| {
                if policy == ReferencePolicy::Acked && self.is_referenced(id) {
                    return DeleteStatus::StillReferenced;
                }
                if policy == ReferencePolicy::DelRef {
                    self.clear_references(id);
                }
                if self.delete_payload(id, true) {
                    DeleteStatus::Deleted
                } else {
                    DeleteStatus::NotFound
                }
            })
            .collect()
    }

    /// Atomic under one mutable borrow. A missing group or missing group-PEL ID
    /// returns NotFound; an existing payload alone is insufficient for XACKDEL.
    pub fn xackdel(
        &mut self,
        group: &[u8],
        ids: &[StreamId],
        policy: ReferencePolicy,
    ) -> Vec<DeleteStatus> {
        ids.iter()
            .map(|&id| {
                let acknowledged = self
                    .groups
                    .get_mut(group)
                    .and_then(|g| g.remove_pending(&id.to_key()))
                    .is_some();
                if !acknowledged {
                    return DeleteStatus::NotFound;
                }
                if policy == ReferencePolicy::Acked && self.is_referenced(id) {
                    return DeleteStatus::StillReferenced;
                }
                if policy == ReferencePolicy::DelRef {
                    self.clear_references(id);
                }
                // A PEL record for an already-deleted payload still succeeds.
                self.delete_payload(id, true);
                DeleteStatus::Deleted
            })
            .collect()
    }

    /// Trims live entries, respecting the selected consumer-reference policy.
    /// Approximate mode is a documented per-entry fallback, with an examination
    /// budget; it does not replicate Redis listpack allocation boundaries.
    pub fn xtrim(&mut self, options: TrimOptions) -> usize {
        let limit = match options.mode {
            TrimMode::Exact => usize::MAX,
            TrimMode::Approximate { limit: None } => 10_000,
            TrimMode::Approximate { limit: Some(0) } => usize::MAX,
            TrimMode::Approximate { limit: Some(n) } => n,
        };
        let mut remaining = self.entries.len();
        let mut victims = Vec::new();
        for (examined, (key, _)) in self.entries.iter().enumerate() {
            if examined >= limit {
                break;
            }
            let id = StreamId::from_key(*key);
            match options.strategy {
                TrimStrategy::MaxLen(max) if remaining <= max => break,
                TrimStrategy::MinId(min) if id >= min => break,
                _ => {}
            }
            if options.references == ReferencePolicy::Acked && self.is_referenced(id) {
                continue;
            }
            victims.push(id);
            remaining -= 1;
        }
        for &id in &victims {
            if options.references == ReferencePolicy::DelRef {
                self.clear_references(id);
            }
            // Like XTRIM, prefix removal does not advance the explicit-XDEL marker.
            self.delete_payload(id, false);
        }
        victims.len()
    }

    /// The struct itself is an existing stream; MKSTREAM belongs to a keyspace layer.
    /// None leaves entries_read unknown; Some is clamped to entries_added.
    pub fn xgroup_create(
        &mut self,
        name: &[u8],
        start: GroupStart,
        entries_read: Option<u64>,
    ) -> Result<()> {
        if self.groups.contains_key(name) {
            return Err(StreamError::GroupExists);
        }
        let id = self.resolve_group_start(start);
        let read = entries_read.map(|n| n.min(self.entries_added));
        self.groups.insert(name.to_vec(), Group::new(id, read));
        Ok(())
    }

    /// Move the delivery cursor without touching any pending entries.
    /// As with CREATE, omitted entries_read leaves the logical counter unknown.
    pub fn xgroup_setid(
        &mut self,
        name: &[u8],
        start: GroupStart,
        entries_read: Option<u64>,
    ) -> Result<()> {
        let id = self.resolve_group_start(start);
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        group.last_delivered = id;
        group.entries_read = entries_read.map(|n| n.min(self.entries_added));
        Ok(())
    }

    pub fn xgroup_destroy(&mut self, name: &[u8]) -> bool {
        self.groups.remove(name).is_some()
    }

    pub fn xgroup_createconsumer(&mut self, name: &[u8], consumer: &[u8]) -> Result<bool> {
        let now = self.clock.now_ms();
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        if group.consumers.contains_key(consumer) {
            return Ok(false);
        }
        group
            .consumers
            .insert(consumer.to_vec(), Consumer::new(now));
        Ok(true)
    }

    /// Removes the consumer and its PEL records; does not requeue their payloads.
    /// Claim or acknowledge outstanding work before deleting a consumer.
    pub fn xgroup_delconsumer(&mut self, name: &[u8], consumer: &[u8]) -> Result<usize> {
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        let Some(removed) = group.consumers.remove(consumer) else {
            return Ok(0);
        };
        let count = removed.pel.len();
        for key in removed.pel.keys() {
            let _ = group.pel.remove(key);
        }
        Ok(count)
    }

    pub fn xreadgroup(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        mode: GroupRead,
        options: ReadGroupOptions,
    ) -> Result<Vec<GroupEntry>> {
        let now = self.clock.now_ms();
        let meta = self.meta();
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        group.touch(consumer, now);
        let limit = options.count.unwrap_or(usize::MAX);
        let mut result = Vec::new();
        match mode {
            GroupRead::New => {
                let keys =
                    keys_in_range(&self.entries, IdRange::after(group.last_delivered), limit);
                for key in keys {
                    let id = StreamId::from_key(key);
                    let fields = self.entries.get(&key).expect("selected live entry");
                    group.advance(id, meta);
                    if !options.no_ack {
                        // SETID rewind / FORCE can make a new read encounter an existing PEL ID.
                        // New delivery resets retry count and moves ownership; it never duplicates it.
                        group.assign(
                            key,
                            Pending {
                                consumer: consumer.to_vec(),
                                delivery_ms: now,
                                deliveries: 1,
                            },
                        );
                        group.active(consumer, now);
                    }
                    result.push(GroupEntry {
                        id,
                        fields: Some(fields.clone()),
                    });
                }
            }
            GroupRead::PendingAfter(after) => {
                let keys = keys_in_range(
                    &group
                        .consumers
                        .get(consumer)
                        .expect("consumer was created")
                        .pel,
                    IdRange::after(after),
                    limit,
                );
                for key in keys {
                    let fields = self.entries.get(&key).cloned();
                    // Deleted payloads are returned as null WITHOUT updating delivery metadata.
                    if fields.is_some() {
                        let p = group.pel.get_mut(&key).expect("consistent consumer PEL");
                        p.delivery_ms = now;
                        p.deliveries = p.deliveries.saturating_add(1);
                        // Redis 8.2 history reads do not update consumer active_time.
                    }
                    result.push(GroupEntry {
                        id: StreamId::from_key(key),
                        fields,
                    });
                }
                // NOACK is intentionally ignored for history reads.
            }
        }
        Ok(result)
    }

    /// Missing group is a zero acknowledgement count, not NOGROUP.
    pub fn xack(&mut self, name: &[u8], ids: &[StreamId]) -> usize {
        let Some(group) = self.groups.get_mut(name) else {
            return 0;
        };
        ids.iter()
            .filter(|id| group.remove_pending(&id.to_key()).is_some())
            .count()
    }

    pub fn xpending(&self, name: &[u8]) -> Result<PendingSummary> {
        let g = self.groups.get(name).ok_or(StreamError::NoGroup)?;
        let mut consumers: Vec<_> = g
            .consumers
            .iter()
            .filter(|(_, c)| !c.pel.is_empty())
            .map(|(name, c)| (name.clone(), c.pel.len()))
            .collect();
        consumers.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(PendingSummary {
            total: g.pel.len(),
            min_id: g.pel.first_key_value().map(|(k, _)| StreamId::from_key(*k)),
            max_id: g.pel.last_key_value().map(|(k, _)| StreamId::from_key(*k)),
            consumers,
        })
    }

    pub fn xpending_range(&self, name: &[u8], query: PendingQuery<'_>) -> Result<Vec<PendingInfo>> {
        let g = self.groups.get(name).ok_or(StreamError::NoGroup)?;
        let Some((lo, hi)) = query.range.normalized() else {
            return Ok(Vec::new());
        };
        let now = self.clock.now_ms();
        let entries = g
            .pel
            .range::<Key, _>((Bound::Included(lo.to_key()), Bound::Included(hi.to_key())))
            .filter(|(_, p)| query.consumer.map_or(true, |c| c == p.consumer.as_slice()))
            .filter(|(_, p)| {
                query
                    .min_idle_ms
                    .map_or(true, |idle| now.saturating_sub(p.delivery_ms) >= idle)
            })
            .take(query.count)
            .map(|(k, p)| pending_info(k, p, now))
            .collect();
        Ok(entries)
    }

    pub fn xclaim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        ids: &[StreamId],
        options: ClaimOptions,
    ) -> Result<ClaimReply> {
        let now = self.clock.now_ms();
        let delivery = match options.delivery_time {
            DeliveryTime::Now => now,
            DeliveryTime::IdleMs(idle) => now.checked_sub(idle).unwrap_or(now),
            DeliveryTime::UnixMs(time) => {
                if time <= now {
                    time
                } else {
                    now
                }
            }
        };
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        group.touch(consumer, now);
        if let Some(id) = options.last_id {
            // LASTID moves forward only; unlike SETID, it preserves entries_read.
            group.last_delivered = group.last_delivered.max(id);
        }
        let mut reply = ClaimReply::empty(options.just_id);
        for &id in ids {
            let key = id.to_key();
            let Some(fields) = self.entries.get(&key) else {
                // Clean tombstones even when min-idle-time has not elapsed.
                let _ = group.remove_pending(&key);
                continue;
            };
            let old = group.pel.get(&key);
            let old_count = match old {
                Some(p) => {
                    if now.saturating_sub(p.delivery_ms) < options.min_idle_ms {
                        continue;
                    }
                    p.deliveries
                }
                None if options.force => 1,
                None => continue,
            };
            let deliveries = options.retry_count.unwrap_or_else(|| {
                if options.just_id {
                    old_count
                } else {
                    old_count.saturating_add(1)
                }
            });
            group.assign(
                key,
                Pending {
                    consumer: consumer.to_vec(),
                    delivery_ms: delivery,
                    deliveries,
                },
            );
            group.active(consumer, now);
            reply.push(id, fields);
        }
        Ok(reply)
    }

    /// Scans <= 10*COUNT PEL records. Both claimed and cleaned-up deleted IDs
    /// consume COUNT; the continuation cursor is the next unexamined PEL ID.
    pub fn xautoclaim(
        &mut self,
        name: &[u8],
        consumer: &[u8],
        options: AutoClaimOptions,
    ) -> Result<AutoClaimReply> {
        if options.count == 0 || options.count > (isize::MAX as usize) / 16 {
            return Err(StreamError::InvalidCount);
        }
        let budget = options
            .count
            .checked_mul(10)
            .ok_or(StreamError::InvalidCount)?;
        let now = self.clock.now_ms();
        let group = self.groups.get_mut(name).ok_or(StreamError::NoGroup)?;
        group.touch(consumer, now);
        let keys = keys_in_range(
            &group.pel,
            IdRange::inclusive(options.start, StreamId::MAX),
            budget,
        );
        let mut claimed = ClaimReply::empty(options.just_id);
        let mut deleted_ids = Vec::new();
        let mut last_scanned = None;
        for key in keys {
            if claimed.len() + deleted_ids.len() == options.count {
                break;
            }
            last_scanned = Some(key);
            let id = StreamId::from_key(key);
            let Some(fields) = self.entries.get(&key) else {
                let _ = group.remove_pending(&key);
                deleted_ids.push(id);
                continue;
            };
            let p = group.pel.get(&key).expect("selected PEL entry");
            if now.saturating_sub(p.delivery_ms) < options.min_idle_ms {
                continue;
            }
            let deliveries = if options.just_id {
                p.deliveries
            } else {
                p.deliveries.saturating_add(1)
            };
            group.assign(
                key,
                Pending {
                    consumer: consumer.to_vec(),
                    delivery_ms: now,
                    deliveries,
                },
            );
            group.active(consumer, now);
            claimed.push(id, fields);
        }
        let next_start = last_scanned
            .and_then(|last| {
                group
                    .pel
                    .range::<Key, _>((Bound::Excluded(last), Bound::Unbounded))
                    .next()
                    .map(|(k, _)| StreamId::from_key(*k))
            })
            .unwrap_or(StreamId::ZERO);
        Ok(AutoClaimReply {
            next_start,
            claimed,
            deleted_ids,
        })
    }

    /// Administrative metadata operation. It is not a normal append API.
    pub fn xsetid(&mut self, id: StreamId, options: SetIdOptions) -> Result<()> {
        if id < self.max_deleted_entry_id {
            return Err(StreamError::InvalidMetadata);
        }
        if self
            .entries
            .last_key_value()
            .is_some_and(|(k, _)| StreamId::from_key(*k) > id)
        {
            return Err(StreamError::InvalidMetadata);
        }
        if options.max_deleted_id.is_some_and(|deleted| deleted > id)
            || options
                .entries_added
                .is_some_and(|n| n < self.entries.len() as u64)
        {
            return Err(StreamError::InvalidMetadata);
        }
        self.last_generated_id = id;
        if let Some(added) = options.entries_added {
            self.entries_added = added;
        }
        if let Some(deleted) = options.max_deleted_id {
            if deleted != StreamId::ZERO {
                self.max_deleted_entry_id = deleted;
            }
        }
        Ok(())
    }

    pub fn xinfo_stream(&self) -> StreamInfo {
        let entry = |(k, fields): (&Key, &Fields)| Entry {
            id: StreamId::from_key(*k),
            fields: fields.clone(),
        };
        StreamInfo {
            length: self.entries.len(),
            groups: self.groups.len(),
            last_generated_id: self.last_generated_id,
            max_deleted_entry_id: self.max_deleted_entry_id,
            entries_added: self.entries_added,
            recorded_first_entry_id: self.meta().first,
            first_entry: self.entries.first_key_value().map(entry),
            last_entry: self.entries.last_key_value().map(entry),
        }
    }

    fn group_info(&self, name: &[u8], group: &Group) -> GroupInfo {
        let unread_entries = match group.last_delivered.successor() {
            Some(start) => self
                .entries
                .range::<Key, _>((Bound::Included(start.to_key()), Bound::Unbounded))
                .count(),
            None => 0,
        };
        GroupInfo {
            name: name.to_vec(),
            consumers: group.consumers.len(),
            pending: group.pel.len(),
            last_delivered_id: group.last_delivered,
            entries_read: group.entries_read,
            lag: self.meta().lag(group),
            unread_entries,
        }
    }

    pub fn xinfo_groups(&self) -> Vec<GroupInfo> {
        let mut result: Vec<_> = self
            .groups
            .iter()
            .map(|(n, g)| self.group_info(n, g))
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    }

    fn consumer_info(name: &[u8], consumer: &Consumer, now: u64) -> ConsumerInfo {
        ConsumerInfo {
            name: name.to_vec(),
            pending: consumer.pel.len(),
            idle_ms: now.saturating_sub(consumer.seen_ms),
            inactive_ms: consumer.active_ms.map(|active| now.saturating_sub(active)),
        }
    }

    pub fn xinfo_consumers(&self, name: &[u8]) -> Result<Vec<ConsumerInfo>> {
        let group = self.groups.get(name).ok_or(StreamError::NoGroup)?;
        let now = self.clock.now_ms();
        let mut result: Vec<_> = group
            .consumers
            .iter()
            .map(|(n, c)| Self::consumer_info(n, c, now))
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    /// `count` independently limits entries, group PELs and consumer PELs.
    /// None means unlimited. All groups/consumers are returned.
    pub fn xinfo_stream_full(&self, count: Option<usize>) -> FullStreamInfo {
        let limit = count.unwrap_or(usize::MAX);
        let now = self.clock.now_ms();
        let mut groups: Vec<_> = self
            .groups
            .iter()
            .map(|(name, g)| {
                let pending = g
                    .pel
                    .iter()
                    .take(limit)
                    .map(|(k, p)| pending_info(k, p, now))
                    .collect();
                let mut consumers: Vec<_> = g
                    .consumers
                    .iter()
                    .map(|(name, c)| FullConsumerInfo {
                        info: Self::consumer_info(name, c, now),
                        pending: c
                            .pel
                            .keys()
                            .take(limit)
                            .map(|k| pending_info(k, g.pel.get(k).expect("consistent PEL"), now))
                            .collect(),
                    })
                    .collect();
                consumers.sort_by(|a, b| a.info.name.cmp(&b.info.name));
                FullGroupInfo {
                    info: self.group_info(name, g),
                    pending,
                    consumers,
                }
            })
            .collect();
        groups.sort_by(|a, b| a.info.name.cmp(&b.info.name));
        FullStreamInfo {
            info: self.xinfo_stream(),
            entries: self.xrange(IdRange::all(), count),
            groups,
        }
    }

    /// Expensive invariant checker for tests and debug tooling, not the hot path.
    pub fn check_invariants(&self) -> std::result::Result<(), String> {
        if self.entries.len() as u64 > self.entries_added {
            return Err("entries_added is below live length".into());
        }
        if self.max_deleted_entry_id > self.last_generated_id {
            return Err("deleted marker exceeds last-generated ID".into());
        }
        for key in self.entries.keys() {
            let id = StreamId::from_key(*key);
            if id == StreamId::ZERO || id > self.last_generated_id {
                return Err("live ID is outside the permitted range".into());
            }
        }
        for group in self.groups.values() {
            for (key, pending) in group.pel.iter() {
                let Some(c) = group.consumers.get(pending.consumer.as_slice()) else {
                    return Err("PEL owner is not a registered consumer".into());
                };
                if !c.pel.contains_key(key) {
                    return Err("group PEL is missing its consumer index".into());
                }
            }
            for (name, consumer) in &group.consumers {
                for key in consumer.pel.keys() {
                    let Some(p) = group.pel.get(key) else {
                        return Err("consumer index is missing its group PEL record".into());
                    };
                    if &p.consumer != name {
                        return Err("PEL owner/index mismatch".into());
                    }
                }
            }
        }
        Ok(())
    }
}

#[path = "snapshot.rs"]
mod snapshot;
#[path = "stream_memory.rs"]
mod stream_memory;
use crate::raft::types::core::structure::stream::clock::{Clock, SystemClock};
use crate::raft::types::core::structure::stream::id::{AddId, GroupStart, IdRange, StreamId};
use crate::raft::types::core::structure::stream::types::{
    AddOptions, AutoClaimOptions, AutoClaimReply, ClaimOptions, ClaimReply, ConsumerInfo,
    DeleteStatus, DeliveryTime, Entry, Fields, FullConsumerInfo, FullGroupInfo, FullStreamInfo,
    GroupEntry, GroupInfo, GroupRead, PendingInfo, PendingQuery, PendingSummary, ReadGroupOptions,
    ReadStart, ReferencePolicy, SetIdOptions, StreamError, StreamInfo, TrimMode, TrimStrategy,
};
pub use snapshot::{
    ConsumerSnapshot, GroupSnapshot, PendingSnapshot, SnapshotError, StreamSnapshot,
    SNAPSHOT_VERSION,
};
