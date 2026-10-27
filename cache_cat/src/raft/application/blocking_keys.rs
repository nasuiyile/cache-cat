use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{self, Instant},
};

type WaiterId = u64;
type WaitResult<K, V> = Result<(K, V), WaitError>;
type SelectState<W, K, V, C> = fn(&mut W) -> &mut BlockingState<K, V, C>;
type Release<W> = fn(&mut W, WaiterId, bool) -> bool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    EmptyKeys,
    Timeout,
    Closed,
}

/// One cancellation record, with no key/context cloning or boxed closure.
/// A live registration can enqueue at most one record on drop. The queue has
/// no fixed capacity/client limit; its memory follows outstanding cleanup.
pub struct Cleanup<W> {
    release: Release<W>,
    id: WaiterId,
    retry: bool,
}

pub type CleanupSender<W> = mpsc::UnboundedSender<Cleanup<W>>;
pub type CleanupReceiver<W> = mpsc::UnboundedReceiver<Cleanup<W>>;

pub fn cleanup_channel<W>() -> (CleanupSender<W>, CleanupReceiver<W>) {
    mpsc::unbounded_channel()
}

/// A registry owned exactly once by the global writer. Mutable data and the
/// weak access descriptor used by its registrations live in this same object.
/// Cloning the registry would duplicate scheduling state, so it is not Clone.
pub struct BlockingKeys<W, K, V, C = ()> {
    state: BlockingState<K, V, C>,
    access: Arc<Access<W, K, V, C>>,
}

struct Access<W, K, V, C> {
    owner: Weak<Mutex<W>>,
    cleanup: CleanupSender<W>,
    select: SelectState<W, K, V, C>,
    release: Release<W>,
}

pub struct Registration<W, K, V, C = ()> {
    access: Arc<Access<W, K, V, C>>,
    id: WaiterId,
    deadline: Option<Instant>,
    rx: Option<oneshot::Receiver<WaitResult<K, V>>>,
    active: bool,
}

/// A signalled consumer retains its exclusive turn until its Raft attempt
/// finishes. Drop returns an unconsumed signal, or completes a consumed turn.
pub struct ExclusiveRegistration<W, K, C = ()> {
    access: Arc<Access<W, K, (), C>>,
    id: WaiterId,
    deadline: Option<Instant>,
    rx: Option<oneshot::Receiver<WaitResult<K, ()>>>,
    active: bool,
    consumed: bool,
}

/// Plain state: callers must hold the owner's write guard. It has no private
/// mutex, atomic waiter counter, or independently mutable shared alias.
pub struct BlockingState<K, V, C = ()> {
    keys: HashMap<K, BTreeSet<WaiterId>>,
    waiters: HashMap<WaiterId, Waiter<K, V, C>>,
    next_id: WaiterId,
    // The final flag identifies key hints (BLPOP) versus per-waiter readiness
    // (consumer groups). Cancelling the latter must not create a blind hint.
    in_flight: Option<(WaiterId, K, Vec<K>, bool)>,
    ready: HashSet<K>,
    eligible: BTreeMap<WaiterId, K>,
}

struct Waiter<K, V, C = ()> {
    keys: Vec<K>,
    deadline: Option<Instant>,
    context: C,
    tx: oneshot::Sender<WaitResult<K, V>>,
}

impl<W, K, V, C> fmt::Debug for BlockingKeys<W, K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.state, f)
    }
}

impl<K, V, C> fmt::Debug for BlockingState<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockingState")
            .field("keys", &self.keys.len())
            .field("waiters", &self.waiters.len())
            .field("in_flight", &self.in_flight.is_some())
            .finish()
    }
}

impl<W, K, V, C> Deref for BlockingKeys<W, K, V, C> {
    type Target = BlockingState<K, V, C>;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<W, K, V, C> DerefMut for BlockingKeys<W, K, V, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<W, K, V, C> BlockingKeys<W, K, V, C> {
    pub fn new(
        owner: Weak<Mutex<W>>,
        cleanup: CleanupSender<W>,
        select: SelectState<W, K, V, C>,
        release: Release<W>,
    ) -> Self {
        Self {
            state: BlockingState::default(),
            access: Arc::new(Access {
                owner,
                cleanup,
                select,
                release,
            }),
        }
    }
}

impl<W: Send + 'static, K, V, C> BlockingKeys<W, K, V, C> {
    /// The owner calls this with its single idle receiver while holding the
    /// global write guard, before registering. Construction itself needs no
    /// runtime. Both registry kinds share the one receiver and cleanup worker.
    pub fn start_cleanup(
        &self,
        receiver: &mut Option<CleanupReceiver<W>>,
    ) -> Result<(), WaitError> {
        self.access.start_cleanup(receiver)
    }
}

impl<W: Send + 'static, K, V, C> Access<W, K, V, C> {
    fn start_cleanup(&self, receiver: &mut Option<CleanupReceiver<W>>) -> Result<(), WaitError> {
        if self.cleanup.is_closed() {
            return Err(WaitError::Closed);
        }
        if receiver.is_some() {
            let runtime = tokio::runtime::Handle::try_current().map_err(|_| WaitError::Closed)?;
            let mut receiver = receiver.take().expect("receiver checked above");
            let owner = self.owner.clone();
            runtime.spawn(async move {
                while let Some(event) = receiver.recv().await {
                    // Do not retain the state machine while waiting for events.
                    let Some(owner) = owner.upgrade() else {
                        return;
                    };
                    let mut guard = owner.lock().await;
                    (event.release)(&mut guard, event.id, event.retry);
                    // Amortize bursts, but let queued apply/registration tasks
                    // acquire the FIFO Tokio mutex after at most 64 cleanups.
                    let mut processed = 1;
                    for _ in 1..64 {
                        let Ok(event) = receiver.try_recv() else {
                            break;
                        };
                        (event.release)(&mut guard, event.id, event.retry);
                        processed += 1;
                    }
                    drop(guard);
                    drop(owner);
                    if processed == 64 {
                        // A permanently ready channel must not monopolize a
                        // single-thread runtime before other writers can poll.
                        tokio::task::yield_now().await;
                    }
                }
            });
        }
        Ok(())
    }
}

impl<W, K, V, C> Access<W, K, V, C> {
    fn defer_release(&self, id: WaiterId, retry: bool) {
        // Receiver closure means the owner/runtime is shutting down. Normal
        // completion and timeout remove state directly, without using the queue.
        let _ = self.cleanup.send(Cleanup {
            release: self.release,
            id,
            retry,
        });
    }
}

impl<W, K, V, C> BlockingKeys<W, K, V, C>
where
    W: Send + 'static,
    K: Eq + Hash + Clone,
{
    /// The owner prepares cleanup, checks data and registers under one global
    /// write guard. No await or independently acquired lock separates them.
    pub fn register_with(
        &mut self,
        keys: Vec<K>,
        deadline: Option<Instant>,
        context: C,
    ) -> Result<Registration<W, K, V, C>, WaitError> {
        let keys = dedup_keys(keys);
        if keys.is_empty() {
            return Err(WaitError::EmptyKeys);
        }
        if self.access.cleanup.is_closed() {
            return Err(WaitError::Closed);
        }
        let state = &mut self.state;
        let (tx, rx) = oneshot::channel();
        let id = state.alloc_id();
        for key in &keys {
            state.keys.entry(key.clone()).or_default().insert(id);
        }
        state.waiters.insert(
            id,
            Waiter {
                keys,
                deadline,
                context,
                tx,
            },
        );
        Ok(Registration {
            access: self.access.clone(),
            id,
            deadline,
            rx: Some(rx),
            active: true,
        })
    }
}

impl<W, K, C> BlockingKeys<W, K, (), C>
where
    W: Send + 'static,
    K: Eq + Hash + Clone,
{
    pub fn register_exclusive_with(
        &mut self,
        keys: Vec<K>,
        deadline: Option<Instant>,
        context: C,
    ) -> Result<ExclusiveRegistration<W, K, C>, WaitError> {
        let mut registration = self.register_with(keys, deadline, context)?;
        registration.active = false;
        Ok(ExclusiveRegistration {
            access: registration.access.clone(),
            id: registration.id,
            deadline,
            rx: registration.rx.take(),
            active: true,
            consumed: false,
        })
    }
}

impl<W, K, V> BlockingKeys<W, K, V>
where
    W: Send + 'static,
    K: Eq + Hash + Clone,
{
    pub fn register(
        &mut self,
        keys: Vec<K>,
        deadline: Option<Instant>,
    ) -> Result<Registration<W, K, V>, WaitError> {
        self.register_with(keys, deadline, ())
    }
}

impl<K, V, C> Default for BlockingState<K, V, C> {
    fn default() -> Self {
        Self {
            keys: HashMap::new(),
            waiters: HashMap::new(),
            next_id: 0,
            in_flight: None,
            ready: HashSet::new(),
            eligible: BTreeMap::new(),
        }
    }
}

impl<K, V, C> BlockingState<K, V, C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty() && self.in_flight.is_none()
    }

    /// Enumerate only registered keys, for commands such as FLUSHDB/FLUSHALL.
    /// Ordinary writes never need to scan this index.
    pub fn registered_keys(&self) -> impl Iterator<Item = &K> {
        self.keys.keys()
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.waiters.len() + usize::from(self.in_flight.is_some())
    }
}

impl<K, V, C> BlockingState<K, V, C>
where
    K: Eq + Hash + Clone,
{
    pub fn wake_one(&mut self, key: &K, mut value: V) -> Result<(), V> {
        loop {
            let Some(id) = self.keys.get(key).and_then(|queue| queue.first().copied()) else {
                return Err(value);
            };
            let waiter = self.remove_waiter(id).expect("inconsistent waiter state");
            if waiter
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                let _ = waiter.tx.send(Err(WaitError::Timeout));
                continue;
            }
            match waiter.tx.send(Ok((key.clone(), value))) {
                Ok(()) => return Ok(()),
                Err(Ok((_, returned))) => value = returned,
                Err(Err(_)) => unreachable!("sent a successful notification"),
            }
        }
    }

    pub fn wake_all(&mut self, key: &K, value: V) -> usize
    where
        V: Clone,
    {
        let Some(ids) = self.keys.remove(key) else {
            return 0;
        };
        let now = Instant::now();
        let mut notified = 0;
        for id in ids {
            let waiter = self.remove_waiter(id).expect("inconsistent waiter state");
            if waiter.deadline.is_some_and(|deadline| now >= deadline) {
                let _ = waiter.tx.send(Err(WaitError::Timeout));
            } else if waiter.tx.send(Ok((key.clone(), value.clone()))).is_ok() {
                notified += 1;
            }
        }
        notified
    }

    /// Resolve a command's responses while the caller holds the global writer.
    /// Callbacks may read the cache directly, but must not reacquire that lock.
    pub fn wake_ready(&mut self, key: &K, mut resolve: impl FnMut(&C) -> Option<V>) -> usize {
        let Some(ids) = self.keys.get(key).cloned() else {
            return 0;
        };
        let now = Instant::now();
        let mut notified = 0;
        for id in ids {
            let waiter = self.waiters.get(&id).expect("inconsistent waiter state");
            let result = if waiter.deadline.is_some_and(|deadline| now >= deadline) {
                Some(Err(WaitError::Timeout))
            } else {
                resolve(&waiter.context).map(|value| Ok((key.clone(), value)))
            };
            if let Some(result) = result {
                let waiter = self.remove_waiter(id).expect("inconsistent waiter state");
                let ready = result.is_ok();
                if waiter.tx.send(result).is_ok() && ready {
                    notified += 1;
                }
            }
        }
        notified
    }
}

impl<K, C> BlockingState<K, (), C>
where
    K: Eq + Hash + Clone,
{
    pub fn signal_one(&mut self, key: &K) -> usize {
        if self.is_empty() {
            return 0;
        }
        if self.keys.contains_key(key)
            || self
                .in_flight
                .as_ref()
                .is_some_and(|(_, _, keys, _)| keys.contains(key))
        {
            self.ready.insert(key.clone());
        }
        self.dispatch_pending()
    }

    pub fn signal_many(&mut self, keys: impl IntoIterator<Item = K>) -> usize {
        if self.is_empty() {
            return 0;
        }
        for key in keys {
            if self.keys.contains_key(&key)
                || self
                    .in_flight
                    .as_ref()
                    .is_some_and(|(_, _, keys, _)| keys.contains(&key))
            {
                self.ready.insert(key);
            }
        }
        self.dispatch_pending()
    }

    /// Re-evaluate consumers affected by this atomic batch, then signal only
    /// the oldest eligible waiter. Unlike a key hint, eligibility may differ
    /// for consumers of the same key (for example different stream groups).
    /// The caller holds the global writer and must refresh every affected key
    /// after applying the previously signalled request, before finishing it.
    pub fn signal_ready_many(
        &mut self,
        keys: impl IntoIterator<Item = K>,
        mut is_ready: impl FnMut(&K, &C) -> bool,
    ) -> usize {
        if self.is_empty() {
            return 0;
        }
        let mut affected = BTreeMap::new();
        for key in keys {
            if let Some(ids) = self.keys.get(&key) {
                for &id in ids {
                    affected.entry(id).or_insert_with(|| key.clone());
                }
            }
        }
        for (id, key) in affected {
            let waiter = self.waiters.get(&id).expect("inconsistent waiter state");
            if waiter
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                let waiter = self.remove_waiter(id).expect("inconsistent waiter state");
                let _ = waiter.tx.send(Err(WaitError::Timeout));
            } else if is_ready(&key, &waiter.context) {
                self.eligible.insert(id, key);
            } else {
                // A previous request may have exhausted this group, even
                // though another group on the same stream still has entries.
                self.eligible.remove(&id);
            }
        }
        self.dispatch_pending()
    }

    pub fn release_exclusive(&mut self, id: WaiterId, retry: bool) -> bool {
        let removed = if self
            .in_flight
            .as_ref()
            .is_some_and(|(active, _, _, _)| *active == id)
        {
            let (_, key, _, key_hint) = self.in_flight.take().unwrap();
            if retry && key_hint && self.keys.contains_key(&key) {
                self.ready.insert(key);
            }
            true
        } else {
            self.remove_waiter(id).is_some()
        };
        self.dispatch_pending();
        removed
    }

    fn dispatch_pending(&mut self) -> usize {
        if self.in_flight.is_some() {
            return 0;
        }
        loop {
            self.ready.retain(|key| self.keys.contains_key(key));
            let key_hint = self
                .ready
                .iter()
                .filter_map(|key| {
                    self.keys
                        .get(key)?
                        .first()
                        .map(|id| (*id, key.clone(), true))
                })
                .min_by_key(|(id, _, _)| *id);
            let eligible = self
                .eligible
                .first_key_value()
                .map(|(&id, key)| (id, key.clone(), false));
            let next = match (key_hint, eligible) {
                (Some(left), Some(right)) => Some(if left.0 < right.0 { left } else { right }),
                (left, right) => left.or(right),
            };
            let Some((id, key, key_hint)) = next else {
                return 0;
            };
            let waiter = self.remove_waiter(id).expect("inconsistent waiter state");
            if waiter
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                let _ = waiter.tx.send(Err(WaitError::Timeout));
                continue;
            }
            if key_hint {
                self.ready.remove(&key);
            }
            self.in_flight = Some((id, key.clone(), waiter.keys, key_hint));
            if waiter.tx.send(Ok((key.clone(), ()))).is_ok() {
                return 1;
            }
            self.in_flight = None;
            if key_hint {
                self.ready.insert(key);
            }
        }
    }
}

impl<W, K, V, C> Registration<W, K, V, C>
where
    W: Send + 'static,
    K: Eq + Hash,
{
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub fn id(&self) -> WaiterId {
        self.id
    }

    pub async fn cancel(&mut self) -> bool {
        if !self.active {
            return false;
        }
        let Some(owner) = self.access.owner.upgrade() else {
            self.active = false;
            return false;
        };
        let mut owner = owner.lock().await;
        let removed = (self.access.release)(&mut owner, self.id, true);
        self.active = false;
        removed
    }

    pub async fn wait(mut self) -> WaitResult<K, V> {
        let mut rx = self.rx.take().expect("registration already consumed");
        let result = if let Some(deadline) = self.deadline {
            tokio::select! {
                result = &mut rx => result.unwrap_or(Err(WaitError::Closed)),
                _ = time::sleep_until(deadline) => {
                    let expired = if let Some(owner) = self.access.owner.upgrade() {
                        let mut owner = owner.lock().await;
                        (self.access.select)(&mut owner).cancel(self.id)
                    } else { return Err(WaitError::Closed); };
                    if expired { Err(WaitError::Timeout) }
                    else { rx.await.unwrap_or(Err(WaitError::Closed)) }
                }
            }
        } else {
            rx.await.unwrap_or(Err(WaitError::Closed))
        };
        self.active = false;
        result
    }
}

impl<W, K, V, C> Drop for Registration<W, K, V, C> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.access.defer_release(self.id, true);
        }
    }
}

impl<W, K, C> ExclusiveRegistration<W, K, C>
where
    W: Send + 'static,
    K: Eq + Hash,
{
    pub async fn wait(&mut self) -> WaitResult<K, ()> {
        let rx = self.rx.as_mut().expect("registration already consumed");
        let result = if let Some(deadline) = self.deadline {
            tokio::select! {
                result = &mut *rx => result.unwrap_or(Err(WaitError::Closed)),
                _ = time::sleep_until(deadline) => {
                    let expired = if let Some(owner) = self.access.owner.upgrade() {
                        let mut owner = owner.lock().await;
                        let removed = (self.access.select)(&mut owner).cancel(self.id);
                        if removed {
                            // Complete cancellation and dispatch successors
                            // under this same guard; finish needs no second lock.
                            (self.access.release)(&mut owner, self.id, false);
                            self.active = false;
                        }
                        removed
                    } else { return Err(WaitError::Closed); };
                    // A signal that already claimed this turn wins the race.
                    if expired { Err(WaitError::Timeout) }
                    else { rx.await.unwrap_or(Err(WaitError::Closed)) }
                }
            }
        } else {
            rx.await.unwrap_or(Err(WaitError::Closed))
        };
        self.rx = None;
        if result.is_err() {
            self.finish().await;
        }
        result
    }

    pub async fn finish(&mut self) -> bool {
        // This assignment precedes the first await. If the caller is cancelled
        // while queued on the writer, Drop still completes a consumed attempt.
        self.consumed = true;
        self.release(false).await
    }

    pub async fn cancel(&mut self) -> bool {
        self.release(!self.consumed).await
    }

    async fn release(&mut self, retry: bool) -> bool {
        if !self.active {
            return false;
        }
        let Some(owner) = self.access.owner.upgrade() else {
            self.active = false;
            return false;
        };
        let mut owner = owner.lock().await;
        let removed = (self.access.release)(&mut owner, self.id, retry);
        self.active = false;
        removed
    }
}

impl<W, K, C> Drop for ExclusiveRegistration<W, K, C> {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.access.defer_release(self.id, !self.consumed);
        }
    }
}

impl<K, V, C> BlockingState<K, V, C>
where
    K: Eq + Hash,
{
    /// Check the existing key index without scanning registered consumers.
    /// A selected in-flight request no longer needs a readiness notification.
    pub fn observes_key(&self, key: &K) -> bool {
        self.keys.contains_key(key)
    }

    pub fn cancel(&mut self, id: WaiterId) -> bool {
        self.remove_waiter(id).is_some()
    }

    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).expect("waiter id overflow");
        id
    }

    fn remove_waiter(&mut self, id: WaiterId) -> Option<Waiter<K, V, C>> {
        let waiter = self.waiters.remove(&id)?;
        self.eligible.remove(&id);
        for key in &waiter.keys {
            let empty = match self.keys.get_mut(key) {
                Some(queue) => {
                    queue.remove(&id);
                    queue.is_empty()
                }
                None => false,
            };
            if empty {
                self.keys.remove(key);
            }
        }
        Some(waiter)
    }
}

fn dedup_keys<K: Eq + Hash + Clone>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::with_capacity(keys.len());
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct TestOwner<V, C = ()> {
        state: BlockingKeys<Self, &'static str, V, C>,
        cleanup: Option<CleanupReceiver<Self>>,
        starts: usize,
    }

    impl<V: Send + 'static, C: Send + 'static> TestOwner<V, C> {
        fn prepare(&mut self) -> Result<(), WaitError> {
            let idle = self.cleanup.is_some();
            self.state.start_cleanup(&mut self.cleanup)?;
            self.starts += usize::from(idle);
            Ok(())
        }
    }

    struct TestBlocking<V, C = ()> {
        owner: Arc<Mutex<TestOwner<V, C>>>,
    }

    impl<V: Send + 'static, C: Send + 'static> TestBlocking<V, C> {
        fn with_release(release: Release<TestOwner<V, C>>) -> Self {
            let (tx, rx) = cleanup_channel();
            let owner = Arc::new_cyclic(|weak| {
                Mutex::new(TestOwner {
                    state: BlockingKeys::new(weak.clone(), tx, |owner| &mut *owner.state, release),
                    cleanup: Some(rx),
                    starts: 0,
                })
            });
            Self { owner }
        }

        fn new() -> Self {
            Self::with_release(|owner, id, _| owner.state.cancel(id))
        }

        fn register_with(
            &self,
            keys: Vec<&'static str>,
            deadline: Option<Instant>,
            context: C,
        ) -> Result<Registration<TestOwner<V, C>, &'static str, V, C>, WaitError> {
            let mut owner = self.owner.try_lock().unwrap();
            owner.prepare()?;
            owner.state.register_with(keys, deadline, context)
        }

        fn wake_one(&self, key: &&'static str, value: V) -> Result<(), V> {
            self.owner.try_lock().unwrap().state.wake_one(key, value)
        }

        fn wake_all(&self, key: &&'static str, value: V) -> usize
        where
            V: Clone,
        {
            self.owner.try_lock().unwrap().state.wake_all(key, value)
        }

        fn wake_ready(&self, key: &&'static str, resolve: impl FnMut(&C) -> Option<V>) -> usize {
            self.owner
                .try_lock()
                .unwrap()
                .state
                .wake_ready(key, resolve)
        }

        fn is_empty(&self) -> bool {
            self.owner.try_lock().unwrap().state.is_empty()
        }
    }

    impl<V: Send + 'static> TestBlocking<V> {
        fn register(
            &self,
            keys: Vec<&'static str>,
            deadline: Option<Instant>,
        ) -> Result<Registration<TestOwner<V>, &'static str, V>, WaitError> {
            self.register_with(keys, deadline, ())
        }

        async fn wait(
            &self,
            keys: Vec<&'static str>,
            timeout: Duration,
        ) -> WaitResult<&'static str, V> {
            let registration = self.register(keys, Some(Instant::now() + timeout))?;
            registration.wait().await
        }
    }

    impl<C: Send + 'static> TestBlocking<(), C> {
        fn exclusive() -> Self {
            Self::with_release(|owner, id, retry| owner.state.release_exclusive(id, retry))
        }

        fn register_exclusive_with(
            &self,
            keys: Vec<&'static str>,
            deadline: Option<Instant>,
            context: C,
        ) -> Result<ExclusiveRegistration<TestOwner<(), C>, &'static str, C>, WaitError> {
            let mut owner = self.owner.try_lock().unwrap();
            owner.prepare()?;
            owner.state.register_exclusive_with(keys, deadline, context)
        }

        fn signal_one(&self, key: &&'static str) -> usize {
            self.owner.try_lock().unwrap().state.signal_one(key)
        }

        fn signal_many(&self, keys: impl IntoIterator<Item = &'static str>) -> usize {
            self.owner.try_lock().unwrap().state.signal_many(keys)
        }

        fn signal_ready_many(
            &self,
            keys: impl IntoIterator<Item = &'static str>,
            ready: impl FnMut(&&'static str, &C) -> bool,
        ) -> usize {
            self.owner
                .try_lock()
                .unwrap()
                .state
                .signal_ready_many(keys, ready)
        }
    }

    async fn assert_empty<V, C>(blocking: &TestBlocking<V, C>) {
        time::timeout(Duration::from_secs(1), async {
            loop {
                {
                    let owner = blocking.owner.lock().await;
                    let state = &owner.state;
                    if state.keys.is_empty()
                        && state.waiters.is_empty()
                        && state.in_flight.is_none()
                        && state.ready.is_empty()
                        && state.eligible.is_empty()
                    {
                        assert_eq!(state.waiter_count(), 0);
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup must run without another producer");
    }
    #[tokio::test]
    async fn empty_keys_are_rejected() {
        let blocking = TestBlocking::<()>::new();
        assert!(matches!(
            blocking.register(vec![], None),
            Err(WaitError::EmptyKeys)
        ));
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn indefinite_registration_only_finishes_when_notified() {
        let blocking = TestBlocking::new();
        let registration = blocking.register(vec!["stream"], None).unwrap();
        assert_eq!(registration.deadline(), None);
        let task = tokio::spawn(registration.wait());
        time::advance(Duration::from_secs(86_400)).await;
        assert!(!task.is_finished());
        assert_eq!(blocking.wake_one(&"stream", 7), Ok(()));
        assert_eq!(task.await.unwrap(), Ok(("stream", 7)));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn duplicate_keys_are_registered_once_and_all_keys_are_removed() {
        let blocking = TestBlocking::new();
        let registration = blocking.register(vec!["a", "a", "b"], None).unwrap();
        {
            let owner = blocking.owner.try_lock().unwrap();
            let state = &owner.state;
            assert_eq!(state.keys["a"].len(), 1);
            assert_eq!(state.keys["b"].len(), 1);
            assert_eq!(state.waiters[&registration.id()].keys, vec!["a", "b"]);
        }
        assert_eq!(blocking.wake_one(&"b", 3), Ok(()));
        assert_eq!(registration.wait().await, Ok(("b", 3)));
        assert_eq!(blocking.wake_one(&"a", 4), Err(4));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn wake_one_preserves_fifo_after_cancellation() {
        let blocking = TestBlocking::new();
        let first = blocking.register(vec!["a"], None).unwrap();
        let mut second = blocking.register(vec!["a", "b"], None).unwrap();
        let third = blocking.register(vec!["a"], None).unwrap();
        assert!(second.cancel().await);
        assert!(!second.cancel().await);
        assert_eq!(second.wait().await, Err(WaitError::Closed));
        assert_eq!(blocking.wake_one(&"a", 1), Ok(()));
        assert_eq!(blocking.wake_one(&"a", 2), Ok(()));
        assert_eq!(first.wait().await, Ok(("a", 1)));
        assert_eq!(third.wait().await, Ok(("a", 2)));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn dropping_registration_removes_every_key() {
        let blocking = TestBlocking::<()>::new();
        let registration = blocking.register(vec!["a", "b", "a"], None).unwrap();
        drop(registration);
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_removes_every_key_without_a_notification() {
        let blocking = TestBlocking::<()>::new();
        assert_eq!(
            blocking.wait(vec!["a", "b"], Duration::from_secs(5)).await,
            Err(WaitError::Timeout)
        );
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_one_skips_expired_waiters_and_reports_timeout() {
        let blocking = TestBlocking::new();
        let expired = blocking
            .register(vec!["a", "b"], Some(Instant::now()))
            .unwrap();
        let live = blocking.register(vec!["a"], None).unwrap();
        assert_eq!(blocking.wake_one(&"a", 5), Ok(()));
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(live.wait().await, Ok(("a", 5)));
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn notification_before_deadline_wins_even_when_polled_after_deadline() {
        let blocking = TestBlocking::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let registration = blocking.register(vec!["a"], Some(deadline)).unwrap();
        assert_eq!(registration.deadline(), Some(deadline));
        assert_eq!(blocking.wake_one(&"a", 1), Ok(()));
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(registration.wait().await, Ok(("a", 1)));
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn notification_after_deadline_returns_value_and_reports_timeout() {
        let blocking = TestBlocking::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let registration = blocking.register(vec!["a"], Some(deadline)).unwrap();
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(blocking.wake_one(&"a", 1), Err(1));
        assert_eq!(registration.wait().await, Err(WaitError::Timeout));
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_all_broadcasts_once_and_cleans_up_other_keys() {
        let blocking = TestBlocking::new();
        let first = blocking.register(vec!["a", "b", "a"], None).unwrap();
        let second = blocking.register(vec!["b", "a"], None).unwrap();
        let expired = blocking
            .register(vec!["a", "c"], Some(Instant::now()))
            .unwrap();
        let unrelated = blocking.register(vec!["d"], None).unwrap();
        assert_eq!(blocking.wake_all(&"a", 8), 2);
        assert_eq!(first.wait().await, Ok(("a", 8)));
        assert_eq!(second.wait().await, Ok(("a", 8)));
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(blocking.wake_all(&"b", 9), 0);
        assert_eq!(blocking.wake_all(&"c", 9), 0);

        let next = blocking.register(vec!["a"], None).unwrap();
        assert_eq!(blocking.owner.try_lock().unwrap().state.waiters.len(), 2);
        assert_eq!(blocking.wake_all(&"a", 10), 1);
        assert_eq!(next.wait().await, Ok(("a", 10)));
        assert_eq!(blocking.wake_all(&"d", 11), 1);
        assert_eq!(unrelated.wait().await, Ok(("d", 11)));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn wake_all_does_not_include_registrations_created_during_delivery() {
        let blocking = TestBlocking::new();
        let first = blocking.register(vec!["a"], None).unwrap();
        let second = blocking.register(vec!["a"], None).unwrap();
        let mut guard = blocking.owner.lock().await;
        let new_registration = tokio::spawn({
            let owner = blocking.owner.clone();
            async move {
                let mut guard = owner.lock().await;
                guard.prepare().unwrap();
                guard.state.register(vec!["a"], None).unwrap()
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(guard.state.wake_all(&"a", 8), 2);
        drop(guard);
        assert_eq!(first.wait().await, Ok(("a", 8)));
        assert_eq!(second.wait().await, Ok(("a", 8)));
        let mut next = new_registration.await.unwrap();
        assert!(matches!(
            next.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        next.cancel().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn wake_ready_preserves_each_requests_result_and_leaves_unready_waiters() {
        struct Request {
            after: u64,
            count: usize,
        }

        let blocking = TestBlocking::new();
        let first = blocking
            .register_with(vec!["a", "b"], None, Request { after: 0, count: 1 })
            .unwrap();
        let second = blocking
            .register_with(vec!["a"], None, Request { after: 1, count: 2 })
            .unwrap();
        let pending = blocking
            .register_with(
                vec!["a", "b"],
                None,
                Request {
                    after: 10,
                    count: 2,
                },
            )
            .unwrap();
        let mut entries = vec![1, 2, 3];
        assert_eq!(
            blocking.wake_ready(&"a", |request| {
                let result: Vec<_> = entries
                    .iter()
                    .copied()
                    .filter(|id| *id > request.after)
                    .take(request.count)
                    .collect();
                (!result.is_empty()).then_some(result)
            }),
            2
        );

        // A later delete cannot replace the already captured responses.
        entries.clear();
        assert_eq!(first.wait().await, Ok(("a", vec![1])));
        assert_eq!(second.wait().await, Ok(("a", vec![2, 3])));
        {
            let owner = blocking.owner.try_lock().unwrap();
            let state = &owner.state;
            assert_eq!(state.waiters.len(), 1);
            assert_eq!(
                state.keys["a"].iter().copied().collect::<Vec<_>>(),
                vec![pending.id()]
            );
            assert_eq!(
                state.keys["b"].iter().copied().collect::<Vec<_>>(),
                vec![pending.id()]
            );
        }
        assert_eq!(blocking.wake_ready(&"a", |_| None), 0);
        assert_eq!(
            blocking.wake_ready(&"b", |request| Some(vec![request.after + 1])),
            1
        );
        assert_eq!(pending.wait().await, Ok(("b", vec![11])));
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wake_ready_expires_without_resolving_and_preserves_other_deadlines() {
        let blocking = TestBlocking::new();
        let expired = blocking
            .register_with(vec!["a", "b"], Some(Instant::now()), 0)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let pending = blocking
            .register_with(vec!["a", "b"], Some(deadline), 1)
            .unwrap();
        let ready = blocking.register_with(vec!["a"], None, 2).unwrap();
        let mut resolved = Vec::new();
        assert_eq!(
            blocking.wake_ready(&"a", |context| {
                resolved.push(*context);
                (*context == 2).then_some("ready")
            }),
            1
        );
        assert_eq!(resolved, vec![1, 2]);
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(ready.wait().await, Ok(("a", "ready")));
        assert_eq!(pending.deadline(), Some(deadline));

        time::advance(Duration::from_secs(5)).await;
        assert_eq!(
            blocking.wake_ready(&"b", |_| panic!("expired context must not be resolved")),
            0
        );
        assert_eq!(pending.wait().await, Err(WaitError::Timeout));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn exclusive_signals_wait_for_completion_and_preserve_fifo() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut third = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        assert!(!blocking.is_empty());
        assert_eq!(blocking.signal_one(&"a"), 1);
        assert_eq!(first.wait().await, Ok(("a", ())));

        // Consecutive producers may run before the first Raft write finishes.
        // They retain one hint, without allowing either successor to overtake.
        assert_eq!(blocking.signal_one(&"a"), 0);
        assert_eq!(blocking.signal_one(&"a"), 0);
        assert!(matches!(
            second.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(first.finish().await);
        assert!(!first.finish().await);
        assert_eq!(second.wait().await, Ok(("a", ())));
        assert!(matches!(
            third.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // Successful pops signal remaining data before releasing their turn.
        assert_eq!(blocking.signal_one(&"a"), 0);
        second.finish().await;
        assert_eq!(third.wait().await, Ok(("a", ())));
        third.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn exclusive_batch_signals_choose_fifo_before_dispatching_any_key() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a", "b"], None, ())
            .unwrap();
        // EXEC/Lua touched keys may arrive in either hash iteration order.
        // Signalling b alone would let the later multi-key request pop a first.
        assert_eq!(blocking.signal_many(["b", "a"]), 1);
        assert_eq!(first.wait().await, Ok(("a", ())));
        assert!(matches!(
            second.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        first.finish().await;
        assert_eq!(second.wait().await, Ok(("b", ())));
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn exclusive_multikey_consumers_cannot_overtake_on_another_key() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a", "b"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["b", "c"], None, ())
            .unwrap();
        let mut third = blocking
            .register_exclusive_with(vec!["c"], None, ())
            .unwrap();
        assert_eq!(blocking.signal_one(&"a"), 1);
        assert_eq!(blocking.signal_one(&"c"), 0);
        assert_eq!(blocking.signal_one(&"b"), 0);
        assert_eq!(first.wait().await, Ok(("a", ())));
        first.finish().await;
        let (key, ()) = second.wait().await.unwrap();
        assert!(key == "b" || key == "c");
        assert!(matches!(
            third.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        blocking.signal_one(&"c");
        second.finish().await;
        assert_eq!(third.wait().await, Ok(("c", ())));
        third.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn dropping_notified_consumer_hands_unconsumed_signal_to_next() {
        let blocking = TestBlocking::exclusive();
        let first = blocking
            .register_exclusive_with(vec!["a", "b"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        blocking.signal_one(&"a");
        // Simulate disconnect winning select before the signal is polled.
        drop(first);
        assert_eq!(second.wait().await, Ok(("a", ())));
        assert!(!blocking.is_empty());
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn active_consumer_keeps_hints_for_waiters_registered_before_completion() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        blocking.signal_one(&"a");
        assert_eq!(first.wait().await, Ok(("a", ())));
        assert!(!blocking.is_empty());
        assert_eq!(blocking.signal_one(&"a"), 0);
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        first.finish().await;
        assert_eq!(second.wait().await, Ok(("a", ())));
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn finishing_empty_attempt_does_not_recycle_a_stale_signal() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        blocking.signal_one(&"a");
        assert_eq!(first.wait().await, Ok(("a", ())));
        first.finish().await;
        assert!(matches!(
            second.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(second);
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn exclusive_deadlines_skip_expired_waiters_and_keep_claimed_turn() {
        let blocking = TestBlocking::exclusive();
        let mut expired = blocking
            .register_exclusive_with(vec!["a"], Some(Instant::now()), ())
            .unwrap();
        let mut live = blocking
            .register_exclusive_with(vec!["a"], Some(Instant::now() + Duration::from_secs(1)), ())
            .unwrap();
        assert_eq!(blocking.signal_one(&"a"), 1);
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(live.wait().await, Ok(("a", ())));
        live.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn exclusive_timeout_and_cancel_remove_all_keys() {
        let blocking = TestBlocking::exclusive();
        let mut timed = blocking
            .register_exclusive_with(
                vec!["a", "b"],
                Some(Instant::now() + Duration::from_secs(1)),
                (),
            )
            .unwrap();
        assert_eq!(timed.wait().await, Err(WaitError::Timeout));
        assert_empty(&blocking).await;
        let mut cancelled = blocking
            .register_exclusive_with(vec!["a", "b"], None, ())
            .unwrap();
        assert!(cancelled.cancel().await);
        assert!(!cancelled.cancel().await);
        assert_eq!(cancelled.wait().await, Err(WaitError::Closed));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn aborting_exclusive_consumer_releases_the_turn() {
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let task = tokio::spawn(async move {
            first.wait().await.unwrap();
            std::future::pending::<()>().await;
        });
        blocking.signal_one(&"a");
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(second.wait().await, Ok(("a", ())));
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn aborting_wait_future_unregisters_all_keys() {
        let blocking = TestBlocking::<()>::new();
        let registration = blocking.register(vec!["a", "b"], None).unwrap();
        let task = tokio::spawn(registration.wait());
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_empty(&blocking).await;
    }

    #[test]
    fn constructing_owner_and_handles_does_not_require_a_runtime() {
        let blocking = TestBlocking::<()>::new();
        let owner = blocking.owner.try_lock().unwrap();
        assert!(owner.cleanup.is_some());
        assert_eq!(owner.starts, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_waits_for_the_global_writer_before_removing_registration() {
        let blocking = TestBlocking::<()>::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let registration = blocking.register(vec!["a", "b"], Some(deadline)).unwrap();
        let guard = blocking.owner.lock().await;
        let task = tokio::spawn(registration.wait());
        time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert_eq!(guard.state.waiter_count(), 1);
        drop(guard);
        assert_eq!(task.await.unwrap(), Err(WaitError::Timeout));
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn dropping_a_cancelled_finish_does_not_recycle_a_consumed_hint() {
        use futures::FutureExt;
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["a"], None, ())
            .unwrap();
        blocking.signal_one(&"a");
        first.wait().await.unwrap();
        let guard = blocking.owner.lock().await;
        // Poll finish until it queues on the global writer, then cancel it as
        // the connection select would. Drop must remember the consumed turn.
        assert!(first.finish().now_or_never().is_none());
        assert!(first.consumed);
        drop(first);
        drop(guard);
        time::timeout(Duration::from_secs(1), async {
            loop {
                if blocking.owner.lock().await.state.in_flight.is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            second.rx.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(blocking.signal_one(&"a"), 1);
        second.wait().await.unwrap();
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn cleanup_releases_the_writer_after_a_bounded_batch() {
        let blocking = TestBlocking::<()>::new();
        let registrations = (0..129)
            .map(|_| blocking.register(vec!["a"], None).unwrap())
            .collect::<Vec<_>>();
        let guard = blocking.owner.lock().await;
        assert_eq!(guard.starts, 1, "all registrations share one cleanup task");
        drop(registrations);
        // The cleanup task consumes its first message and queues on the lock.
        tokio::task::yield_now().await;
        let observing_writer = tokio::spawn({
            let owner = blocking.owner.clone();
            async move { owner.lock().await.state.waiter_count() }
        });
        tokio::task::yield_now().await;
        drop(guard);
        assert_eq!(observing_writer.await.unwrap(), 65);
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn handles_and_cleanup_worker_do_not_keep_the_owner_alive() {
        let blocking = TestBlocking::<()>::new();
        let registration = blocking.register(vec!["a"], None).unwrap();
        let weak = Arc::downgrade(&blocking.owner);
        let access = registration.access.clone();
        drop(blocking);
        assert!(weak.upgrade().is_none());
        assert_eq!(registration.wait().await, Err(WaitError::Closed));
        drop(access);
    }

    #[tokio::test]
    async fn group_readiness_skips_an_older_unready_group_on_the_same_key() {
        use futures::FutureExt;
        let blocking = TestBlocking::exclusive();
        let mut empty_group = blocking
            .register_exclusive_with(vec!["stream"], None, "empty")
            .unwrap();
        let mut ready_group = blocking
            .register_exclusive_with(vec!["stream"], None, "ready")
            .unwrap();
        assert_eq!(
            blocking.signal_ready_many(["stream"], |_, group| *group == "ready"),
            1
        );
        assert!(empty_group.wait().now_or_never().is_none());
        assert_eq!(ready_group.wait().now_or_never(), Some(Ok(("stream", ()))));
        ready_group.finish().await;
        assert!(empty_group.wait().now_or_never().is_none());
        assert_eq!(blocking.signal_ready_many(["stream"], |_, _| true), 1);
        assert_eq!(empty_group.wait().now_or_never(), Some(Ok(("stream", ()))));
        empty_group.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn group_readiness_is_refreshed_before_handing_the_turn_to_another_consumer() {
        use futures::FutureExt;
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["stream"], None, "group-a")
            .unwrap();
        let mut second = blocking
            .register_exclusive_with(vec!["stream"], None, "group-a")
            .unwrap();
        let mut other_group = blocking
            .register_exclusive_with(vec!["stream"], None, "group-b")
            .unwrap();
        assert_eq!(blocking.signal_ready_many(["stream"], |_, _| true), 1);
        assert_eq!(first.wait().now_or_never(), Some(Ok(("stream", ()))));
        // Another XADD before the selected request commits must not let the
        // second consumer overtake it, regardless of how many are eligible.
        assert_eq!(blocking.signal_ready_many(["stream"], |_, _| true), 0);
        assert!(second.wait().now_or_never().is_none());
        assert!(other_group.wait().now_or_never().is_none());
        // Applying the first request exhausted only group-a. Refreshing the
        // same stream removes group-a's cached eligibility, not group-b's.
        assert_eq!(
            blocking.signal_ready_many(["stream"], |_, group| *group == "group-b"),
            0
        );
        first.finish().await;
        assert!(second.wait().now_or_never().is_none());
        assert_eq!(other_group.wait().now_or_never(), Some(Ok(("stream", ()))));
        other_group.finish().await;
        assert!(second.wait().now_or_never().is_none());
        assert_eq!(blocking.signal_ready_many(["stream"], |_, _| true), 1);
        assert_eq!(second.wait().now_or_never(), Some(Ok(("stream", ()))));
        second.finish().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test]
    async fn cancelled_filtered_turn_hands_off_only_to_eligible_waiters() {
        use futures::FutureExt;
        let blocking = TestBlocking::exclusive();
        let mut first = blocking
            .register_exclusive_with(vec!["a", "b"], None, true)
            .unwrap();
        let mut unready = blocking
            .register_exclusive_with(vec!["a"], None, false)
            .unwrap();
        let mut successor = blocking
            .register_exclusive_with(vec!["b"], None, true)
            .unwrap();
        assert_eq!(blocking.signal_ready_many(["a", "b"], |_, ready| *ready), 1);
        assert!(first.wait().now_or_never().unwrap().is_ok());
        assert!(first.cancel().await);
        assert!(unready.wait().now_or_never().is_none());
        assert_eq!(successor.wait().now_or_never(), Some(Ok(("b", ()))));
        successor.finish().await;
        assert!(unready.wait().now_or_never().is_none());
        unready.cancel().await;
        assert_empty(&blocking).await;
    }

    #[tokio::test(start_paused = true)]
    async fn filtered_waiters_expire_without_resolving_or_losing_other_groups() {
        use futures::FutureExt;
        let blocking = TestBlocking::exclusive();
        let mut expired = blocking
            .register_exclusive_with(vec!["stream"], Some(Instant::now()), 0)
            .unwrap();
        let mut live = blocking
            .register_exclusive_with(vec!["stream"], None, 1)
            .unwrap();
        assert_eq!(
            blocking.signal_ready_many(["stream"], |_, group| {
                assert_eq!(*group, 1, "expired waiters must not inspect stream data");
                true
            }),
            1
        );
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(live.wait().now_or_never(), Some(Ok(("stream", ()))));
        live.finish().await;
        assert_empty(&blocking).await;
    }
}
