mod test;

use crate::utils::glob::GlobMatcher;
use crate::utils::{OptionalU64, now_ms};
use crossbeam_channel::{Receiver, Sender, bounded, select, unbounded};
use papaya::{Compute, Equivalent, HashMap, LocalGuard, Operation};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::Hash;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use tokio::io;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

const WHEEL_BITS: usize = 8;
const WHEEL_SIZE: usize = 1 << WHEEL_BITS;
const WHEEL_MASK: u64 = (WHEEL_SIZE as u64) - 1;
const WHEEL_LEVELS: usize = 8;
const LARGE_ADVANCE_THRESHOLD: u64 = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpirePolicy {
    Persistent,
    Absolute(u64),
    Ttl(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntrySnapshot<V> {
    pub value: V,
    pub expire_at: Option<u64>,
}

impl<V> EntrySnapshot<V> {
    pub fn get_expire_policy(&self) -> ExpirePolicy {
        match self.expire_at {
            None => ExpirePolicy::Persistent,
            Some(v) => ExpirePolicy::Absolute(v),
        }
    }
}

#[derive(Clone, Debug)]
struct Entry<V> {
    value: V,
    expire_at: OptionalU64,
}

impl<V> Entry<V> {
    fn snapshot(&self) -> EntrySnapshot<V>
    where
        V: Clone,
    {
        EntrySnapshot {
            value: self.value.clone(),
            expire_at: self.expire_at.get(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MochaOperation<V> {
    Insert { value: V, expire: ExpirePolicy },
    Remove,
    Abort,
}

#[derive(Clone, Debug)]
struct TimerItem<K> {
    key: K,
    expire_at: u64,
}

#[derive(Debug)]
enum ExpireCommand<K> {
    Schedule { key: K, expire_at: u64 },
    ScheduleBatch { items: Vec<TimerItem<K>> },
    Advance,
    AdvanceAndWait(Sender<()>),
    HasExpiredByLocalClock { now: u64, done: Sender<bool> },
    Pause(Sender<()>),
    ResumeAndWait(Sender<()>),
}

#[derive(Debug)]
struct HierarchicalTimeWheel<K> {
    current: u64,
    wheels: Vec<Vec<Vec<TimerItem<K>>>>,
}

impl<K> HierarchicalTimeWheel<K> {
    fn new(now: u64) -> Self {
        let mut wheels = Vec::with_capacity(WHEEL_LEVELS);

        for _ in 0..WHEEL_LEVELS {
            let mut level = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                level.push(Vec::new());
            }
            wheels.push(level);
        }

        Self {
            current: now,
            wheels,
        }
    }

    fn insert(&mut self, item: TimerItem<K>) {
        if item.expire_at <= self.current {
            self.wheels[0][Self::slot_for(self.current, 0)].push(item);
            return;
        }

        let delta = item.expire_at - self.current;
        let level = Self::level_for_delta(delta);
        let slot = Self::slot_for(item.expire_at, level);

        self.wheels[level][slot].push(item);
    }

    fn has_due<F>(&self, now: u64, mut is_current: F) -> bool
    where
        F: FnMut(&K, u64) -> bool,
    {
        for level in 0..WHEEL_LEVELS {
            for slot in 0..WHEEL_SIZE {
                for item in &self.wheels[level][slot] {
                    if item.expire_at <= now && is_current(&item.key, item.expire_at) {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn advance_to<F>(&mut self, now: u64, mut expire: F)
    where
        F: FnMut(K, u64),
    {
        if now <= self.current {
            return;
        }

        if now.saturating_sub(self.current) > LARGE_ADVANCE_THRESHOLD {
            self.advance_large(now, expire);
            return;
        }

        while self.current < now {
            self.current += 1;

            for level in 1..WHEEL_LEVELS {
                let lower_mask = (1u64 << (level * WHEEL_BITS)) - 1;

                if self.current & lower_mask == 0 {
                    self.cascade(level);
                } else {
                    break;
                }
            }

            let slot = Self::slot_for(self.current, 0);
            let due = std::mem::take(&mut self.wheels[0][slot]);

            for item in due {
                if item.expire_at <= self.current {
                    expire(item.key, item.expire_at);
                } else {
                    self.insert(item);
                }
            }
        }
    }

    fn advance_large<F>(&mut self, now: u64, mut expire: F)
    where
        F: FnMut(K, u64),
    {
        self.current = now;

        let mut pending = Vec::new();

        for level in 0..WHEEL_LEVELS {
            for slot in 0..WHEEL_SIZE {
                pending.extend(std::mem::take(&mut self.wheels[level][slot]));
            }
        }

        for item in pending {
            if item.expire_at <= now {
                expire(item.key, item.expire_at);
            } else {
                self.insert(item);
            }
        }
    }

    fn cascade(&mut self, level: usize) {
        let slot = Self::slot_for(self.current, level);
        let items = std::mem::take(&mut self.wheels[level][slot]);

        for item in items {
            self.insert(item);
        }
    }

    fn level_for_delta(delta: u64) -> usize {
        let mut level = 0;
        let mut span = WHEEL_SIZE as u64;

        while level + 1 < WHEEL_LEVELS && delta >= span {
            level += 1;
            span <<= WHEEL_BITS;
        }

        level
    }

    fn slot_for(at: u64, level: usize) -> usize {
        ((at >> (level * WHEEL_BITS)) & WHEEL_MASK) as usize
    }
}

#[derive(Clone, Debug)]
pub struct Mocha<K, V>
where
    K: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    map: Arc<HashMap<K, Entry<V>>>,
    logic_clock: Arc<AtomicU64>,
    expire_tx: Sender<ExpireCommand<K>>,
}

impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(logic_clock: Arc<AtomicU64>) -> Self {
        let (expire_tx, expire_rx) = unbounded();
        let map = Arc::new(HashMap::new());

        Self::spawn_active_expirer(map.clone(), logic_clock.clone(), expire_rx);

        Self {
            map,
            logic_clock,
            expire_tx,
        }
    }

    fn now_logical(&self) -> u64 {
        self.logic_clock.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn now_local() -> u64 {
        now_ms()
    }

    fn resolve_expire_at(&self, policy: ExpirePolicy) -> OptionalU64 {
        // MAX is reserved for persistent entries. Saturate finite deadlines
        // at MAX - 1; Redis deadlines are already limited to i64::MAX.
        match policy {
            ExpirePolicy::Persistent => OptionalU64::NONE,
            ExpirePolicy::Absolute(at) => OptionalU64::some_saturating(at),
            ExpirePolicy::Ttl(ttl) => {
                OptionalU64::some_saturating(self.now_logical().saturating_add(ttl))
            }
        }
    }

    fn make_entry(&self, value: V, policy: ExpirePolicy) -> Entry<V> {
        Entry {
            value,
            expire_at: self.resolve_expire_at(policy),
        }
    }

    fn enqueue_expiry(&self, key: K, expire_at: OptionalU64) {
        if let Some(expire_at) = expire_at.get() {
            let _ = self
                .expire_tx
                .send(ExpireCommand::Schedule { key, expire_at });
        }
    }

    pub fn insert_entry(&self, key: K, value: V, policy: ExpirePolicy) -> EntrySnapshot<V> {
        let new_entry = self.make_entry(value, policy);
        let snapshot = new_entry.snapshot();
        let expire_at = new_entry.expire_at;
        let mg = self.map.pin();
        let old_entry = mg.insert(key.clone(), new_entry);
        // The old timer also covers a replacement with the same deadline.
        // Use the entry actually replaced, without another map lookup.
        if expire_at.is_some() && old_entry.is_none_or(|old| old.expire_at != expire_at) {
            self.enqueue_expiry(key, expire_at);
        }
        snapshot
    }

    fn remove_expired_if_current_from(
        map: &HashMap<K, Entry<V>>,
        logic_clock: &AtomicU64,
        key: K,
        expire_at: u64,
    ) -> bool {
        let now = logic_clock.load(Ordering::Relaxed);
        let mg = map.pin();
        matches!(
            mg.compute(key, |entry| match entry {
                Some((_, entry))
                    if entry.expire_at.get() == Some(expire_at) && now >= expire_at =>
                {
                    Operation::Remove
                }
                _ => Operation::Abort(()),
            }),
            Compute::Removed(_, _)
        )
    }

    fn has_expired_if_current_from(map: &HashMap<K, Entry<V>>, key: &K, expire_at: u64) -> bool {
        let mg = map.pin();

        matches!(
            mg.get(key),
            Some(entry) if entry.expire_at.get() == Some(expire_at)
        )
    }

    pub fn insert_snapshot(&self, key: K, snapshot: EntrySnapshot<V>) -> EntrySnapshot<V> {
        let policy = match snapshot.expire_at {
            None => ExpirePolicy::Persistent,
            Some(at) => ExpirePolicy::Absolute(at),
        };

        self.insert_entry(key, snapshot.value, policy)
    }

    pub fn insert(&self, key: K, value: V, ttl: u64) -> EntrySnapshot<V> {
        self.insert_entry(key, value, ExpirePolicy::Ttl(ttl))
    }

    pub fn insert_absolute(&self, key: K, value: V, expire_at: u64) -> EntrySnapshot<V> {
        self.insert_entry(key, value, ExpirePolicy::Absolute(expire_at))
    }

    pub fn insert_persistent(&self, key: K, value: V) -> EntrySnapshot<V> {
        self.insert_entry(key, value, ExpirePolicy::Persistent)
    }

    pub fn get_entry<Q>(&self, key: &Q) -> Option<EntrySnapshot<V>>
    where
        Q: ?Sized + Hash + Equivalent<K>,
    {
        let now = self.now_logical();
        let mg = self.map.pin();
        let (expired_key, expired_at) = match mg.get_key_value(key) {
            Some((stored_key, entry)) => match entry.expire_at.get() {
                Some(at) if now >= at => (stored_key.clone(), at),
                _ => return Some(entry.snapshot()),
            },
            None => return None,
        };
        mg.compute(expired_key, |entry| match entry {
            Some((_, entry)) if entry.expire_at.get() == Some(expired_at) && now >= expired_at => {
                Operation::Remove
            }
            _ => Operation::Abort(()),
        });
        None
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).map(|entry| entry.value)
    }
    pub fn get_with_read_clock<Q>(
        &self,
        key: &Q,
        read_clock: Option<u64>,
    ) -> Option<EntrySnapshot<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        match self.get_entry(key) {
            None => {
                //用写逻辑时钟也获取不到 可能会产生写逻辑时钟在此刻推进了导致读不到数据。但这是符合预期的。
                None
            }
            Some(my_value) => {
                match my_value.expire_at {
                    Some(inner) => {
                        match read_clock {
                            None => Some(my_value),
                            Some(time) => {
                                if inner <= time {
                                    // 写逻辑时钟获取到了但是读逻辑时钟没有获取到
                                    return None;
                                }
                                Some(my_value)
                            }
                        }
                    }
                    None => Some(my_value),
                }
            }
        }
    }

    pub fn get_if_alive(&self, key: &K) -> Option<V> {
        self.get_entry(key).map(|entry| entry.value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).is_some()
    }

    pub fn ttl_remaining(&self, key: &K) -> Option<u64> {
        let entry = self.get_entry(key)?;
        let expire_at = entry.expire_at?;

        Some(expire_at.saturating_sub(self.now_logical()))
    }

    fn set_expire_policy(&self, key: &K, policy: ExpirePolicy) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();
        let mg = self.map.pin();
        let result = mg.compute(key.clone(), |entry| match entry {
            Some((_, entry)) if entry.expire_at.get().is_some_and(|at| now >= at) => {
                Operation::Remove
            }
            Some((_, entry)) => Operation::Insert(self.make_entry(entry.value.clone(), policy)),
            None => Operation::Abort(()),
        });
        match result {
            Compute::Updated {
                old: (_, old),
                new: (_, entry),
            } => {
                if entry.expire_at.is_some() && old.expire_at != entry.expire_at {
                    self.enqueue_expiry(key.clone(), entry.expire_at);
                }
                Some(entry.snapshot())
            }
            _ => None,
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.remove_entry(key).map(|entry| entry.value)
    }

    pub fn remove_entry(&self, key: &K) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();

        let mg = self.map.pin();
        let removed = mg.remove(key)?;
        let expired = removed.expire_at.get().is_some_and(|at| now >= at);

        if expired {
            None
        } else {
            Some(removed.snapshot())
        }
    }

    fn spawn_active_expirer(
        map: Arc<HashMap<K, Entry<V>>>,
        logic_clock: Arc<AtomicU64>,
        expire_rx: Receiver<ExpireCommand<K>>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            Self::expire_worker(map, logic_clock, expire_rx);
        })
    }

    fn expire_worker(
        map: Arc<HashMap<K, Entry<V>>>,
        logic_clock: Arc<AtomicU64>,
        expire_rx: Receiver<ExpireCommand<K>>,
    ) {
        let mut wheel = HierarchicalTimeWheel::new(logic_clock.load(Ordering::Relaxed));
        let mut paused = false;

        loop {
            select! {
                recv(expire_rx) -> msg => {
                    let Ok(cmd) = msg else {
                        return;
                    };

                    Self::handle_expire_command(&map, &logic_clock, &mut wheel, &mut paused, cmd);

                    while let Ok(cmd) = expire_rx.try_recv() {
                        Self::handle_expire_command(&map, &logic_clock, &mut wheel, &mut paused, cmd);
                    }
                }
            }
        }
    }

    fn handle_expire_command(
        map: &HashMap<K, Entry<V>>,
        logic_clock: &AtomicU64,
        wheel: &mut HierarchicalTimeWheel<K>,
        paused: &mut bool,
        cmd: ExpireCommand<K>,
    ) {
        match cmd {
            ExpireCommand::Pause(done) => {
                *paused = true;
                // All timers from the pre-restore map are stale. Starting at
                // zero also handles a snapshot whose logical clock is older
                // than this node's clock.
                *wheel = HierarchicalTimeWheel::new(0);
                let _ = done.send(());
            }
            ExpireCommand::ResumeAndWait(done) => {
                *paused = false;
                // Visit every pending timer, including deadlines at zero.
                wheel.advance_large(logic_clock.load(Ordering::Relaxed), |key, expire_at| {
                    Self::remove_expired_if_current_from(map, logic_clock, key, expire_at);
                });
                let _ = done.send(());
            }
            cmd if *paused => match cmd {
                // Keep schedules generated while loading the snapshot. They
                // are evaluated only after the replay has completed.
                ExpireCommand::Schedule { key, expire_at } => {
                    wheel.insert(TimerItem { key, expire_at });
                }
                ExpireCommand::ScheduleBatch { items } => {
                    for item in items {
                        wheel.insert(item);
                    }
                }
                ExpireCommand::Advance => {}
                ExpireCommand::AdvanceAndWait(done) => {
                    let _ = done.send(());
                }
                ExpireCommand::HasExpiredByLocalClock { done, .. } => {
                    let _ = done.send(false);
                }
                ExpireCommand::Pause(done) | ExpireCommand::ResumeAndWait(done) => {
                    let _ = done.send(());
                }
            },
            ExpireCommand::Schedule { key, expire_at } => {
                Self::advance_wheel(map, logic_clock, wheel);

                let now = logic_clock.load(Ordering::Relaxed);

                if expire_at <= now {
                    Self::remove_expired_if_current_from(map, logic_clock, key, expire_at);
                } else {
                    wheel.insert(TimerItem { key, expire_at });
                }
            }
            ExpireCommand::ScheduleBatch { items } => {
                Self::advance_wheel(map, logic_clock, wheel);

                let now = logic_clock.load(Ordering::Relaxed);

                for item in items {
                    if item.expire_at <= now {
                        Self::remove_expired_if_current_from(
                            map,
                            logic_clock,
                            item.key,
                            item.expire_at,
                        );
                    } else {
                        wheel.insert(item);
                    }
                }
            }
            ExpireCommand::Advance => {
                Self::advance_wheel(map, logic_clock, wheel);
            }
            ExpireCommand::AdvanceAndWait(done) => {
                Self::advance_wheel(map, logic_clock, wheel);
                let _ = done.send(());
            }
            ExpireCommand::HasExpiredByLocalClock { now, done } => {
                let has_expired = wheel.has_due(now, |key, expire_at| {
                    Self::has_expired_if_current_from(map, key, expire_at)
                });

                let _ = done.send(has_expired);
            }
        }
    }

    fn advance_wheel(
        map: &HashMap<K, Entry<V>>,
        logic_clock: &AtomicU64,
        wheel: &mut HierarchicalTimeWheel<K>,
    ) {
        let now = logic_clock.load(Ordering::Relaxed);

        wheel.advance_to(now, |key, expire_at| {
            Self::remove_expired_if_current_from(map, logic_clock, key, expire_at);
        });
    }

    pub fn trigger_expire_cycle(&self) {
        let _ = self.expire_tx.send(ExpireCommand::Advance);
    }

    /// Pause physical expiration while a snapshot replaces the map. The
    /// worker acknowledges only after all commands sent before this call have
    /// been processed, so no old timer can race with snapshot loading.
    pub fn pause_expire_worker_blocking(&self) {
        let (done_tx, done_rx) = bounded(1);
        if self.expire_tx.send(ExpireCommand::Pause(done_tx)).is_ok() {
            let _ = done_rx.recv();
        }
    }

    /// Resume expiration after snapshot data and its incremental queue have
    /// been restored. The acknowledgement is sent after one expiration pass
    /// at the restored logical clock.
    pub fn resume_expire_worker_blocking(&self) {
        let (done_tx, done_rx) = bounded(1);
        if self
            .expire_tx
            .send(ExpireCommand::ResumeAndWait(done_tx))
            .is_ok()
        {
            let _ = done_rx.recv();
        }
    }

    pub async fn active_expire_cycle(&self) {
        let (done_tx, done_rx) = bounded(1);

        if self
            .expire_tx
            .send(ExpireCommand::AdvanceAndWait(done_tx))
            .is_err()
        {
            return;
        }

        let _ = tokio::task::spawn_blocking(move || {
            let _ = done_rx.recv();
        })
        .await;
    }

    pub fn active_expire_cycle_blocking(&self) {
        let (done_tx, done_rx) = bounded(1);

        if self
            .expire_tx
            .send(ExpireCommand::AdvanceAndWait(done_tx))
            .is_err()
        {
            return;
        }

        let _ = done_rx.recv();
    }

    pub fn has_expired_by_local_clock(&self) -> bool {
        let (done_tx, done_rx) = bounded(1);

        if self
            .expire_tx
            .send(ExpireCommand::HasExpiredByLocalClock {
                now: Self::now_local(),
                done: done_tx,
            })
            .is_err()
        {
            return false;
        }
        done_rx.recv().unwrap_or(false)
    }
    pub async fn has_expired_by_local_clock_async(&self) -> bool {
        let (done_tx, done_rx) = bounded(1);

        if self
            .expire_tx
            .send(ExpireCommand::HasExpiredByLocalClock {
                now: Self::now_local(),
                done: done_tx,
            })
            .is_err()
        {
            return false;
        }
        tokio::task::spawn_blocking(move || done_rx.recv().unwrap_or(false))
            .await
            .unwrap_or(false)
    }

    pub fn guard(&self) -> LocalGuard<'_> {
        self.map.guard()
    }

    pub async fn dump_snapshots_to_writer<W>(&self, writer: &mut W) -> Result<u64, io::Error>
    where
        W: AsyncWrite + AsyncSeek + Unpin + Send,
        K: Serialize,
        V: Serialize,
    {
        let count_pos = writer.seek(SeekFrom::Current(0)).await?;
        writer.write_u64(0).await?;
        let mut entry_count = 0u64;
        let map = self.map.pin_owned();
        let now = self.now_logical();
        for (key, entry) in map.iter() {
            if entry.expire_at.get().is_some_and(|at| now >= at) {
                continue;
            }
            let snapshot = entry.snapshot();
            let key_bytes = bincode2::serialize(key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let val_bytes = bincode2::serialize(&snapshot)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.write_u64(key_bytes.len() as u64).await?;
            writer.write_all(&key_bytes).await?;
            writer.write_u64(val_bytes.len() as u64).await?;
            writer.write_all(&val_bytes).await?;
            entry_count = entry_count
                .checked_add(1)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "too many entries"))?;
        }
        let end_pos = writer.seek(SeekFrom::Current(0)).await?;
        writer.seek(SeekFrom::Start(count_pos)).await?;
        writer.write_u64(entry_count).await?;
        writer.seek(SeekFrom::Start(end_pos)).await?;
        Ok(entry_count)
    }

    pub fn keys(&self, pattern: &[u8], read_clock: Option<u64>) -> Vec<K>
    where
        K: AsRef<[u8]>,
    {
        let matcher = GlobMatcher::new(pattern);

        // 和 dump_snapshots_to_writer 一样，
        // 整次遍历固定使用同一个写逻辑时钟。
        let write_clock = self.now_logical();
        let map = self.map.pin();
        map.iter()
            .filter_map(|(key, entry)| {
                if let Some(expire_at) = entry.expire_at.get() {
                    // 第一层：按照当前写逻辑时钟判断。
                    // 对应 get_entry：
                    // write_clock >= expire_at 表示当前已经过期。
                    if write_clock >= expire_at {
                        return None;
                    }

                    // 第二层：按照传入的读逻辑时钟判断。
                    // 对应 get_with_read_clock：
                    // expire_at < read_clock 表示在该读时钟下不可见。
                    if read_clock.is_some_and(|clock| clock >= expire_at) {
                        return None;
                    }
                }

                if matcher.matches(key.as_ref()) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn clear(&self) {
        let mg = self.map.pin();
        mg.clear();
    }

    /// Filter stored values without cloning keys or payloads. Snapshot replay
    /// uses this to preserve entries newer than a recorded database flush.
    pub(crate) fn retain_values(&self, mut keep: impl FnMut(&V) -> bool) {
        self.map.pin().retain(|_, entry| keep(&entry.value));
    }

    pub fn unlink_batch(&self, keys: &[K]) -> usize {
        let now = self.now_logical();
        let expire_at = OptionalU64::some_saturating(now);
        let mg = self.map.pin();

        let mut items = Vec::with_capacity(keys.len());

        for key in keys {
            let result = mg.compute(key.clone(), |entry| match entry {
                Some((_, entry)) => {
                    // 已经过期，不重复 unlink
                    if entry.expire_at.get().is_some_and(|at| now >= at) {
                        return Operation::Abort(());
                    }

                    // 逻辑上立即过期
                    let mut new_entry = entry.clone();
                    new_entry.expire_at = expire_at;

                    Operation::Insert(new_entry)
                }
                None => Operation::Abort(()),
            });

            if matches!(result, Compute::Updated { .. }) {
                items.push(TimerItem {
                    key: key.clone(),
                    expire_at: expire_at.into_raw(),
                });
            }
        }

        let count = items.len();

        if count > 0 {
            let _ = self.expire_tx.send(ExpireCommand::ScheduleBatch { items });
        }

        count
    }

    pub fn unlink(&self, key: &K) -> bool {
        let now = self.now_logical();
        let expire_at = OptionalU64::some_saturating(now);
        let mg = self.map.pin();

        let result = mg.compute(key.clone(), |entry| match entry {
            Some((_, entry)) => {
                // 已经过期，视为不存在
                if entry.expire_at.get().is_some_and(|at| now >= at) {
                    return Operation::Abort(());
                }

                // 逻辑删除：立即设置为已过期
                let mut new_entry = entry.clone();
                new_entry.expire_at = expire_at;

                Operation::Insert(new_entry)
            }
            None => Operation::Abort(()),
        });

        if matches!(result, Compute::Updated { .. }) {
            // 交给后台 expire worker 做物理删除
            self.enqueue_expiry(key.clone(), expire_at);
            true
        } else {
            false
        }
    }
    pub fn len(&self) -> usize {
        self.map.pin().len()
    }

    /// Count keys visible at a fixed clock without cloning their keys or values.
    /// The physical map length can differ between replicas while expired entries
    /// await reclamation, so it must not feed replicated state-machine writes.
    pub fn len_with_read_clock(&self, read_clock: u64) -> usize {
        let clock = self.now_logical().max(read_clock);
        self.map
            .pin()
            .iter()
            .filter(|(_, entry)| entry.expire_at.get().is_none_or(|at| clock < at))
            .count()
    }
}
