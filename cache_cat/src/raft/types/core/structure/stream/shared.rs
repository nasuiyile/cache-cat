use crate::raft::types::core::structure::stream::core::*;
use crate::raft::types::core::structure::stream::id::*;
use crate::raft::types::core::structure::stream::memory::*;
use crate::raft::types::core::structure::stream::snapshot::StreamSnapshot;
use crate::raft::types::core::structure::stream::types::*;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::pin,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{Instant, timeout_at},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Block {
    /// Do not wait for data. Lock acquisition can still block the calling thread.
    NoWait,
    /// Total waiting budget, including synchronous lock acquisition.
    /// Duration::ZERO is the same as NoWait.
    For(Duration),
    Forever,
}

impl Block {
    /// Redis BLOCK 0 means forever, not a zero-duration poll.
    pub fn redis_millis(ms: u64) -> Self {
        if ms == 0 {
            Self::Forever
        } else {
            Self::For(Duration::from_millis(ms))
        }
    }
}

/// A batch stops on its first failure; preceding entries remain committed.
/// Entries at and after failed_index are not appended. This is NOT rollback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAddError {
    pub committed: Vec<StreamId>,
    pub failed_index: usize,
    pub error: StreamError,
}

impl fmt::Display for BatchAddError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "batch append failed at index {} after {} commits: {}",
            self.failed_index,
            self.committed.len(),
            self.error
        )
    }
}

impl std::error::Error for BatchAddError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Clone, Copy)]
struct Deadline {
    at: Option<Instant>,
    can_wait: bool,
}

impl Deadline {
    fn new(block: Block) -> Result<Self> {
        match block {
            Block::NoWait => Ok(Self {
                at: None,
                can_wait: false,
            }),
            Block::Forever => Ok(Self {
                at: None,
                can_wait: true,
            }),
            Block::For(d) if d.is_zero() => Ok(Self {
                at: None,
                can_wait: false,
            }),
            Block::For(d) => Ok(Self {
                at: Some(
                    Instant::now()
                        .checked_add(d)
                        .ok_or(StreamError::InvalidTimeout)?,
                ),
                can_wait: true,
            }),
        }
    }

    fn expired(self) -> bool {
        self.at.is_some_and(|at| Instant::now() >= at)
    }

    async fn run<F: Future>(self, future: F) -> Option<F::Output> {
        match self.at {
            Some(at) => {
                if Instant::now() >= at {
                    return None;
                }
                timeout_at(at, future).await.ok()
            }
            None => Some(future.await),
        }
    }

    async fn wait<F: Future<Output = ()>>(self, future: F) -> bool {
        self.can_wait && self.run(future).await.is_some()
    }
}

#[derive(Debug)]
struct GroupSignal {
    identity: Arc<()>,
    changed: Notify,
}

#[derive(Debug)]
struct State {
    stream: RedisStream,
    // Weak handles do not keep group queues alive after the last call exits.
    // Dead entries are reclaimed on the next append/mutation or group deletion.
    groups: HashMap<Vec<u8>, Weak<GroupSignal>>,
    poisoned: bool,
}

impl State {
    fn group_signal(&mut self, name: &[u8], identity: &Arc<()>) -> Arc<GroupSignal> {
        if let Some(signal) = self.groups.get(name).and_then(Weak::upgrade) {
            if Arc::ptr_eq(&signal.identity, identity) {
                return signal;
            }
            signal.changed.notify_waiters();
        }
        let signal = Arc::new(GroupSignal {
            identity: identity.clone(),
            changed: Notify::new(),
        });
        self.groups.insert(name.to_vec(), Arc::downgrade(&signal));
        signal
    }

    fn append_targets(&mut self) -> Vec<Arc<GroupSignal>> {
        let mut targets = Vec::new();
        self.groups.retain(|_, weak| {
            if let Some(signal) = weak.upgrade() {
                targets.push(signal);
                true
            } else {
                false
            }
        });
        targets
    }

    /// Generic mutations may destroy/recreate groups, rewind their cursors,
    /// replace the entire stream, or partially commit before returning Err.
    fn mutation_targets(&mut self) -> Vec<Arc<GroupSignal>> {
        let mut targets = Vec::new();
        let stream = &self.stream;
        self.groups.retain(|name, weak| {
            let Some(signal) = weak.upgrade() else {
                return false;
            };
            let keep = stream
                .group_identity(name)
                .is_ok_and(|identity| Arc::ptr_eq(&identity, &signal.identity));
            targets.push(signal);
            keep
        });
        targets
    }
}

#[derive(Debug)]
struct Inner {
    state: RwLock<State>,
    // XREAD is broadcast: every independent reader must see the append.
    readers: Notify,
}

/// Explicit poisoning: parking_lot locks do not provide std::sync poisoning.
/// A panicking mutation may have changed only part of the core state.
struct WriteGuard<'a> {
    state: RwLockWriteGuard<'a, State>,
    readers: &'a Notify,
}

impl Deref for WriteGuard<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.state
    }
}

impl DerefMut for WriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.state.poisoned = true;
            // Exceptional path only: notify under the lock. Woken tasks cannot
            // inspect the poisoned state until this guard has been released.
            self.readers.notify_waiters();
            for weak in self.state.groups.values() {
                if let Some(signal) = weak.upgrade() {
                    signal.changed.notify_waiters();
                }
            }
        }
    }
}

/// Relay the notification on timeout/cancellation, including deadline expiry
/// while reacquiring the stream write lock after Notify completed.
/// This guard is created BEFORE Notified; Notified is dropped first on exit.
struct WakeRelay {
    signal: Arc<GroupSignal>,
    forward: bool,
}

impl Drop for WakeRelay {
    fn drop(&mut self) {
        if self.forward {
            self.signal.changed.notify_one();
        }
    }
}

/// Single-stream sharing with parking_lot locks and Tokio data notifications.
///
/// - Concurrent pure reads use RwLock::read; all mutations use RwLock::write.
/// - No stream guard is held while waiting for data or any external future.
/// - Normal readers are broadcast; each active group receives one wakeup.
/// - Successful group reads relay the wakeup while retained unread data exists.
/// - Cancellation/timeout relays a possibly consumed group wakeup.
/// - Timed operations share one absolute deadline, including lock acquisition.
/// - Lock contention blocks the calling thread; only data notifications yield.
/// - Only xread_blocking/xreadgroup_blocking are async; other operations execute
///   synchronously and do not require a Tokio runtime.
///
/// inspect/modify callbacks are synchronous. Keep them short, do not perform
/// blocking I/O, and do not re-enter this adapter from a callback. A callback
/// that returns a Future does NOT run that future inside the critical section.
///
/// This is not lock-free. Writes to the same stream remain serialized. Results
/// still clone message fields; the adapter does not implement zero-copy reads.
#[derive(Debug, Clone)]
pub struct SharedStream(Arc<Inner>);

impl Default for SharedStream {
    fn default() -> Self {
        Self::new(RedisStream::new())
    }
}

impl SharedStream {
    pub fn new(stream: RedisStream) -> Self {
        Self(Arc::new(Inner {
            state: RwLock::new(State {
                stream,
                groups: HashMap::new(),
                poisoned: false,
            }),
            readers: Notify::new(),
        }))
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, State>> {
        Self::check_read(self.0.state.read())
    }

    fn check_read(state: RwLockReadGuard<'_, State>) -> Result<RwLockReadGuard<'_, State>> {
        if state.poisoned {
            return Err(StreamError::LockPoisoned);
        }
        Ok(state)
    }

    fn write(&self) -> Result<WriteGuard<'_>> {
        self.check_write(self.0.state.write())
    }

    fn check_write<'a>(&'a self, state: RwLockWriteGuard<'a, State>) -> Result<WriteGuard<'a>> {
        if state.poisoned {
            return Err(StreamError::LockPoisoned);
        }
        Ok(WriteGuard {
            state,
            readers: &self.0.readers,
        })
    }

    fn read_before(&self, deadline: Deadline) -> Result<Option<RwLockReadGuard<'_, State>>> {
        let Some(at) = deadline.at else {
            return self.read().map(Some);
        };
        let Some(remaining) = at.checked_duration_since(Instant::now()) else {
            return Ok(None);
        };
        let state = self.0.state.try_read_for(remaining);
        if deadline.expired() {
            return Ok(None);
        }
        state.map(Self::check_read).transpose()
    }

    fn write_before(&self, deadline: Deadline) -> Result<Option<WriteGuard<'_>>> {
        let Some(at) = deadline.at else {
            return self.write().map(Some);
        };
        let Some(remaining) = at.checked_duration_since(Instant::now()) else {
            return Ok(None);
        };
        let state = self.0.state.try_write_for(remaining);
        if deadline.expired() {
            return Ok(None);
        }
        state.map(|state| self.check_write(state)).transpose()
    }

    fn publish_append(&self, groups: Vec<Arc<GroupSignal>>) {
        // Always called AFTER dropping the write lock, with no intervening await.
        self.0.readers.notify_waiters();
        for group in groups {
            group.changed.notify_one();
        }
    }

    /// Acquire a read lock, then serialize a consistent borrowed view. Message
    /// fields are not cloned. Encoding is synchronous while holding the guard;
    /// a slow serializer blocks writers and can occupy a Tokio runtime worker.
    /// Do not re-enter this stream from the serializer or writer.
    ///
    /// Unlike direct Serialize, this waits synchronously for lock contention.
    /// Output may be partially written if the serializer fails; use a temporary
    /// file + atomic replacement at the application layer for durable snapshots.
    pub fn serialize_with<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let state = self
            .read()
            .map_err(<S::Error as serde::ser::Error>::custom)?;
        serde::Serialize::serialize(&state.stream, serializer)
    }

    /// Clone logical data under a read lock, then release it. The returned owned
    /// snapshot can be encoded/compressed/written without holding a stream lock.
    /// This costs an additional O(data) allocation; serialize_with avoids that.
    pub fn snapshot(&self) -> Result<StreamSnapshot> {
        let state = self.read()?;
        Ok(state.stream.snapshot())
    }

    /// Estimate reachable memory under a read lock using five entries plus the
    /// tail. See MemoryUsage for inclusions/limits. Groups, consumers and the
    /// notification registry are still visited; entry/PEL indexes are modeled.
    /// Reports the underlying stream ONCE, regardless of SharedStream clones.
    pub fn memory_usage(&self) -> Result<MemoryUsage> {
        self.memory_usage_with_samples(DEFAULT_MEMORY_SAMPLES)
    }

    /// Same sampling semantics as RedisStream::memory_usage_with_samples, with
    /// shared allocation and notification overhead included under the read lock.
    pub fn memory_usage_with_samples(&self, samples: usize) -> Result<MemoryUsage> {
        use std::mem::size_of;

        let state = self.read()?;
        let mut usage = state.stream.memory_usage_with_samples(samples);
        // Inner contains State and RedisStream inline. Avoid counting the core
        // inline size twice. This includes fixed lock/Notify and Arc overhead.
        usage.shared_overhead_bytes = size_of::<Self>()
            .saturating_add(arc_bytes::<Inner>())
            .saturating_sub(size_of::<RedisStream>());
        usage.notification_bytes =
            hash_table_bytes::<Vec<u8>, Weak<GroupSignal>>(state.groups.capacity());
        for (name, weak) in &state.groups {
            usage.notification_bytes = usage
                .notification_bytes
                .saturating_add(name.capacity())
                // Even when strong_count == 0, a Weak retains its Arc allocation.
                .saturating_add(arc_bytes::<GroupSignal>());
            if let Some(signal) = weak.upgrade() {
                let identity_in_core = state
                    .stream
                    .group_identity(name)
                    .is_ok_and(|identity| Arc::ptr_eq(&identity, &signal.identity));
                if !identity_in_core {
                    usage.notification_bytes =
                        usage.notification_bytes.saturating_add(arc_bytes::<()>());
                }
            }
        }
        // In-flight task futures and signals removed from this registry are not
        // owned by the stream registry and cannot be enumerated here.
        usage.finish();
        Ok(usage)
    }

    /// Convenience total, in bytes, using the default five-entry sample budget.
    pub fn estimated_memory_usage(&self) -> Result<usize> {
        Ok(self.memory_usage()?.total_bytes)
    }

    pub fn inspect<T>(&self, read: impl FnOnce(&RedisStream) -> T) -> Result<T> {
        let state = self.read()?;
        Ok(read(&state.stream))
    }

    /// General escape hatch. Broadcasts even on Err because earlier mutations
    /// in the same callback may already have succeeded. NOT a transaction.
    /// Prefer dedicated xadd/xack methods in hot paths.
    pub fn modify<T>(&self, mutate: impl FnOnce(&mut RedisStream) -> Result<T>) -> Result<T> {
        let mut state = self.write()?;
        let result = mutate(&mut state.stream);
        let targets = state.mutation_targets();
        drop(state);
        self.0.readers.notify_waiters();
        for target in targets {
            target.changed.notify_waiters();
        }
        result
    }

    pub fn xadd(&self, request: AddId, fields: Fields) -> Result<StreamId> {
        self.xadd_with_options(request, fields, AddOptions::default())
    }

    pub fn xadd_with_options(
        &self,
        request: AddId,
        fields: Fields,
        options: AddOptions,
    ) -> Result<StreamId> {
        let mut state = self.write()?;
        let id = state.stream.xadd_with_options(request, fields, options)?;
        let targets = state.append_targets();
        drop(state);
        self.publish_append(targets);
        Ok(id)
    }

    /// One lock acquisition and one append notification round per batch.
    /// Build the input outside the lock. Keep batches bounded (e.g. 64-256);
    /// an enormous batch monopolizes the runtime worker and stream write lock.
    /// On failure, committed IDs are returned and those commits are notified.
    pub fn xadd_batch(
        &self,
        entries: Vec<(AddId, Fields)>,
        options: AddOptions,
    ) -> std::result::Result<Vec<StreamId>, BatchAddError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut committed = Vec::with_capacity(entries.len());
        let mut entries = entries.into_iter().enumerate();
        let mut state = self.write().map_err(|error| BatchAddError {
            committed: Vec::new(),
            failed_index: 0,
            error,
        })?;
        let mut failure = None;
        for (index, (id, fields)) in entries.by_ref() {
            match state.stream.xadd_with_options(id, fields, options) {
                Ok(id) => committed.push(id),
                Err(error) => {
                    failure = Some((index, error));
                    break;
                }
            }
        }
        let changed = !committed.is_empty();
        let targets = if changed {
            state.append_targets()
        } else {
            Vec::new()
        };
        drop(state);
        if changed {
            self.publish_append(targets);
        }
        match failure {
            Some((failed_index, error)) => Err(BatchAddError {
                committed,
                failed_index,
                error,
            }),
            None => Ok(committed),
        }
    }

    pub fn xlen(&self) -> Result<usize> {
        self.inspect(RedisStream::xlen)
    }

    pub fn xrange(&self, range: IdRange, count: Option<usize>) -> Result<Vec<Entry>> {
        self.inspect(|s| s.xrange(range, count))
    }

    pub fn xrevrange(&self, range: IdRange, count: Option<usize>) -> Result<Vec<Entry>> {
        self.inspect(|s| s.xrevrange(range, count))
    }

    /// ACK does not make new messages available. No reader wakeup is needed.
    pub fn xack(&self, name: &[u8], ids: &[StreamId]) -> Result<usize> {
        let mut state = self.write()?;
        Ok(state.stream.xack(name, ids))
    }

    pub fn xpending(&self, name: &[u8]) -> Result<PendingSummary> {
        self.read()?.stream.xpending(name)
    }

    pub fn xpending_range(&self, name: &[u8], query: PendingQuery<'_>) -> Result<Vec<PendingInfo>> {
        self.read()?.stream.xpending_range(name, query)
    }

    pub fn xdel(&self, ids: &[StreamId]) -> Result<usize> {
        let mut state = self.write()?;
        Ok(state.stream.xdel(ids))
    }

    pub fn xtrim(&self, options: TrimOptions) -> Result<usize> {
        let mut state = self.write()?;
        Ok(state.stream.xtrim(options))
    }

    pub fn xgroup_create(
        &self,
        name: &[u8],
        start: GroupStart,
        entries_read: Option<u64>,
    ) -> Result<()> {
        let mut state = self.write()?;
        state.stream.xgroup_create(name, start, entries_read)
    }

    pub fn xgroup_setid(
        &self,
        name: &[u8],
        start: GroupStart,
        entries_read: Option<u64>,
    ) -> Result<()> {
        let mut state = self.write()?;
        state.stream.xgroup_setid(name, start, entries_read)?;
        let target = state.groups.get(name).and_then(Weak::upgrade);
        drop(state);
        if let Some(target) = target {
            target.changed.notify_one();
        }
        Ok(())
    }

    pub fn xgroup_destroy(&self, name: &[u8]) -> Result<bool> {
        let mut state = self.write()?;
        let removed = state.stream.xgroup_destroy(name);
        let target = state.groups.remove(name).and_then(|weak| weak.upgrade());
        drop(state);
        // ALL calls attached to the old group generation must terminate.
        if let Some(target) = target {
            target.changed.notify_waiters();
        }
        Ok(removed)
    }

    /// Non-blocking with respect to data availability, not OS threads.
    pub fn xread(&self, start: ReadStart, count: Option<usize>) -> Result<Vec<Entry>> {
        Ok(self.read()?.stream.xread_from(start, count))
    }

    /// BLOCK waits asynchronously for data; acquiring the parking_lot lock is
    /// synchronous. `$` is resolved exactly once under the initial read lock.
    pub async fn xread_blocking(
        &self,
        start: ReadStart,
        count: Option<usize>,
        block: Block,
    ) -> Result<Vec<Entry>> {
        let effective = if matches!(start, ReadStart::Latest) || count == Some(0) {
            Block::NoWait
        } else {
            block
        };
        let deadline = Deadline::new(effective)?;
        let mut after = None;
        loop {
            let mut notified = pin!(self.0.readers.notified());
            {
                let Some(state) = self.read_before(deadline)? else {
                    return Ok(Vec::new());
                };
                if matches!(start, ReadStart::Latest) || count == Some(0) {
                    return Ok(state.stream.xread_from(start, count));
                }
                let after = *after.get_or_insert_with(|| match start {
                    ReadStart::After(id) => id,
                    ReadStart::Tail => state.stream.last_generated_id(),
                    ReadStart::Latest => unreachable!("handled above"),
                });
                let entries = state.stream.xread(after, count);
                if !entries.is_empty() {
                    return Ok(entries);
                }
                if !deadline.can_wait || deadline.expired() {
                    return Ok(Vec::new());
                }
                // Predicate + registration share the read lock, so an append
                // cannot fall into a wakeup gap. The guard ends before await.
                notified.as_mut().enable();
            }
            if !deadline.wait(notified.as_mut()).await {
                return Ok(Vec::new());
            }
        }
    }

    pub fn xreadgroup(
        &self,
        name: &[u8],
        consumer: &[u8],
        mode: GroupRead,
        options: ReadGroupOptions,
    ) -> Result<Vec<GroupEntry>> {
        self.write()?
            .stream
            .xreadgroup(name, consumer, mode, options)
    }

    pub async fn xreadgroup_blocking(
        &self,
        name: &[u8],
        consumer: &[u8],
        mode: GroupRead,
        options: ReadGroupOptions,
        block: Block,
    ) -> Result<Vec<GroupEntry>> {
        let immediate = matches!(mode, GroupRead::PendingAfter(_)) || options.count == Some(0);
        let deadline = Deadline::new(if immediate { Block::NoWait } else { block })?;
        let (identity, signal) = {
            let Some(mut state) = self.write_before(deadline)? else {
                return Ok(Vec::new());
            };
            let identity = state.stream.group_identity(name)?;
            let entries = state.stream.xreadgroup(name, consumer, mode, options)?;
            if !entries.is_empty() || immediate || !deadline.can_wait || deadline.expired() {
                return Ok(entries);
            }
            // Allocate a signal only on the empty/blocking path, not on hot reads.
            let signal = state.group_signal(name, &identity);
            (identity, signal)
        };
        let mut relay = WakeRelay {
            signal: signal.clone(),
            forward: true,
        };
        loop {
            let mut notified = pin!(signal.changed.notified());
            {
                let Some(mut state) = self.write_before(deadline)? else {
                    return Ok(Vec::new());
                };
                let current = state.stream.group_identity(name)?;
                if !Arc::ptr_eq(&identity, &current) {
                    return Err(StreamError::GroupRecreated);
                }
                let entries = state.stream.xreadgroup(name, consumer, mode, options)?;
                if !entries.is_empty() {
                    relay.forward = state.stream.group_has_new(name)?;
                    // Guard release and handoff happen in the same poll.
                    return Ok(entries);
                }
                if deadline.expired() {
                    return Ok(Vec::new());
                }
                // Recheck after signal creation and register under the lock.
                // This also covers appends before the first registration.
                notified.as_mut().enable();
            }
            if !deadline.wait(notified.as_mut()).await {
                return Ok(Vec::new());
            }
        }
    }
}

/// Serde is synchronous, so this implementation acquires a read lock only when
/// immediately available. Contention (including a queued writer) returns a
/// serializer error instead of blocking a runtime thread or panicking.
/// Use `serialize_with(...)` or `snapshot()` to wait for the lock
/// synchronously on the calling thread.
impl serde::Serialize for SharedStream {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let state = self.0.state.try_read().ok_or_else(|| {
            <S::Error as serde::ser::Error>::custom(
                "SharedStream read lock is busy; use serialize_with(...) or snapshot()",
            )
        })?;
        if state.poisoned {
            return Err(<S::Error as serde::ser::Error>::custom(
                StreamError::LockPoisoned,
            ));
        }
        serde::Serialize::serialize(&state.stream, serializer)
    }
}

/// Decode and validate a new logical stream, then construct fresh runtime locks
/// and empty notifications. This never overwrites another SharedStream or its
/// clones, and does not restore waiting tasks, Arc identities, or a custom clock.
impl<'de> serde::Deserialize<'de> for SharedStream {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let stream = <RedisStream as serde::Deserialize<'de>>::deserialize(deserializer)?;
        Ok(Self::new(stream))
    }
}

#[cfg(test)]
#[path = "shared_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "shared_serde_tests.rs"]
mod serde_tests;
