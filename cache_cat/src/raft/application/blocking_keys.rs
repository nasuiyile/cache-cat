use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    hash::Hash,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::oneshot,
    time::{self, Instant},
};

type WaiterId = u64;
type WaitResult<K, V> = Result<(K, V), WaitError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitError {
    EmptyKeys,
    Timeout,
    Closed,
}

pub struct BlockingKeys<K, V, C = ()> {
    inner: Arc<Inner<K, V, C>>,
}

pub struct Registration<K, V, C = ()>
where
    K: Eq + Hash,
{
    inner: Arc<Inner<K, V, C>>,
    id: WaiterId,
    deadline: Option<Instant>,
    rx: Option<oneshot::Receiver<WaitResult<K, V>>>,
    active: bool,
}

struct Inner<K, V, C = ()> {
    state: Mutex<State<K, V, C>>,
}

struct State<K, V, C = ()> {
    // IDs increase monotonically, so the smallest ID is the first waiter.
    // A set also allows cancellation without scanning every key's queue.
    keys: HashMap<K, BTreeSet<WaiterId>>,
    waiters: HashMap<WaiterId, Waiter<K, V, C>>,
    next_id: WaiterId,
}

struct Waiter<K, V, C = ()> {
    keys: Vec<K>,
    deadline: Option<Instant>,
    context: C,
    tx: oneshot::Sender<WaitResult<K, V>>,
}

impl<K, V, C> fmt::Debug for BlockingKeys<K, V, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.state.lock();
        f.debug_struct("BlockingKeys")
            .field("keys", &state.keys.len())
            .field("waiters", &state.waiters.len())
            .finish()
    }
}

impl<K, V, C> Clone for BlockingKeys<K, V, C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, C> BlockingKeys<K, V, C>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    keys: HashMap::new(),
                    waiters: HashMap::new(),
                    next_id: 0,
                }),
            }),
        }
    }

    /// Check the data and register while holding the same lock as writers,
    /// so a write cannot slip between the check and this registration. For
    /// notification-only waits, registering before checking also prevents
    /// missed notifications. `None` waits without a deadline.
    pub fn register_with(
        &self,
        keys: Vec<K>,
        deadline: Option<Instant>,
        context: C,
    ) -> Result<Registration<K, V, C>, WaitError> {
        let keys = dedup_keys(keys);

        if keys.is_empty() {
            return Err(WaitError::EmptyKeys);
        }

        let (tx, rx) = oneshot::channel();

        let id = {
            let mut state = self.inner.state.lock();
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

            id
        };

        Ok(Registration {
            inner: self.inner.clone(),
            id,
            deadline,
            rx: Some(rx),
            active: true,
        })
    }

    pub fn wake_one(&self, key: &K, mut value: V) -> Result<(), V> {
        loop {
            let (waiter, expired) = {
                let mut state = self.inner.state.lock();
                let Some(id) = state.keys.get(key).and_then(|queue| queue.first().copied()) else {
                    return Err(value);
                };
                let waiter = state.remove_waiter(id).expect("inconsistent waiter state");
                let expired = waiter
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline);
                (waiter, expired)
            };

            if expired {
                // A receiver may not have polled its timer yet. Dropping its
                // sender here would incorrectly turn an expiry into Closed.
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

    /// Notify all registrations currently waiting on a key, once each.
    ///
    /// Every selected registration is removed from all of its keys before
    /// sending. A receiver that registers again therefore waits for the next
    /// event, even when it runs concurrently with this broadcast.
    pub fn wake_all(&self, key: &K, value: V) -> usize
    where
        V: Clone,
    {
        let waiters = {
            let mut state = self.inner.state.lock();
            let Some(ids) = state.keys.remove(key) else {
                return 0;
            };
            let now = Instant::now();
            ids.into_iter()
                .map(|id| {
                    let waiter = state.remove_waiter(id).expect("inconsistent waiter state");
                    let expired = waiter.deadline.is_some_and(|deadline| now >= deadline);
                    (waiter, expired)
                })
                .collect::<Vec<_>>()
        };

        let mut notified = 0;
        for (waiter, expired) in waiters {
            if expired {
                let _ = waiter.tx.send(Err(WaitError::Timeout));
            } else if waiter.tx.send(Ok((key.clone(), value.clone()))).is_ok() {
                notified += 1;
            }
        }
        notified
    }

    /// Resolve each current registration against the state of this event.
    ///
    /// `None` keeps the registration on all its keys. `Some` captures that
    /// request's response and removes it from every key before delivery, so
    /// subsequent writes cannot change the result observed by the receiver.
    /// Expired registrations receive `Timeout` without invoking `resolve`.
    ///
    /// The callback runs with the registry mutex held. It must not reenter
    /// this registry, cancel its registrations, or acquire locks in reverse
    /// order. Callers reading mutable data must hold the data's write lock
    /// before calling this method and access it directly in the callback.
    pub fn wake_ready(&self, key: &K, mut resolve: impl FnMut(&C) -> Option<V>) -> usize {
        let ready = {
            let mut state = self.inner.state.lock();
            let Some(ids) = state.keys.get(key).cloned() else {
                return 0;
            };
            let now = Instant::now();
            let mut ready = Vec::new();
            for id in ids {
                let waiter = state.waiters.get(&id).expect("inconsistent waiter state");
                let result = if waiter.deadline.is_some_and(|deadline| now >= deadline) {
                    Some(Err(WaitError::Timeout))
                } else {
                    resolve(&waiter.context).map(|value| Ok((key.clone(), value)))
                };
                if let Some(result) = result {
                    let waiter = state.remove_waiter(id).expect("inconsistent waiter state");
                    ready.push((waiter, result));
                }
            }
            ready
        };

        let mut notified = 0;
        for (waiter, result) in ready {
            let is_ready = result.is_ok();
            if waiter.tx.send(result).is_ok() && is_ready {
                notified += 1;
            }
        }
        notified
    }
}

impl<K, V> BlockingKeys<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn register(
        &self,
        keys: Vec<K>,
        deadline: Option<Instant>,
    ) -> Result<Registration<K, V>, WaitError> {
        self.register_with(keys, deadline, ())
    }

    pub async fn wait(&self, keys: Vec<K>, timeout: Duration) -> Result<(K, V), WaitError> {
        self.register(keys, Some(Instant::now() + timeout))?
            .wait()
            .await
    }
}

impl<K, V, C> Default for BlockingKeys<K, V, C>
where
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, C> Registration<K, V, C>
where
    K: Eq + Hash,
{
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn id(&self) -> WaiterId {
        self.id
    }

    pub fn cancel(&mut self) -> bool {
        self.unregister()
    }

    pub async fn wait(mut self) -> Result<(K, V), WaitError> {
        let mut rx = self.rx.take().expect("registration already consumed");

        let Some(deadline) = self.deadline else {
            let result = rx.await.map_err(|_| WaitError::Closed)?;
            self.active = false;
            return result;
        };

        tokio::select! {
            result = &mut rx => {
                self.active = false;
                result.map_err(|_| WaitError::Closed)?
            }

            _ = time::sleep_until(deadline) => {
                if self.unregister() {
                    Err(WaitError::Timeout)
                } else {
                    // The notifier removed this registration first. Its
                    // decision (notification or expiry) wins the race.
                    self.active = false;
                    rx.await.map_err(|_| WaitError::Closed)?
                }
            }
        }
    }

    fn unregister(&mut self) -> bool {
        if !self.active {
            return false;
        }

        self.active = false;

        self.inner.state.lock().remove_waiter(self.id).is_some()
    }
}

impl<K, V, C> Drop for Registration<K, V, C>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if self.active {
            self.inner.state.lock().remove_waiter(self.id);
        }
    }
}

impl<K, V, C> State<K, V, C>
where
    K: Eq + Hash,
{
    fn alloc_id(&mut self) -> WaiterId {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).expect("waiter id overflow");
        id
    }

    fn remove_waiter(&mut self, id: WaiterId) -> Option<Waiter<K, V, C>> {
        let waiter = self.waiters.remove(&id)?;

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

fn dedup_keys<K>(keys: Vec<K>) -> Vec<K>
where
    K: Eq + Hash + Clone,
{
    let mut seen = HashSet::with_capacity(keys.len());

    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_empty<K, V, C>(blocking: &BlockingKeys<K, V, C>) {
        let state = blocking.inner.state.lock();
        assert!(state.keys.is_empty());
        assert!(state.waiters.is_empty());
    }

    #[test]
    fn empty_keys_are_rejected() {
        let blocking = BlockingKeys::<&str, ()>::new();
        assert!(matches!(
            blocking.register(vec![], None),
            Err(WaitError::EmptyKeys)
        ));
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn indefinite_registration_only_finishes_when_notified() {
        let blocking = BlockingKeys::new();
        let registration = blocking.register(vec!["stream"], None).unwrap();
        assert_eq!(registration.deadline(), None);
        let task = tokio::spawn(registration.wait());
        time::advance(Duration::from_secs(86_400)).await;
        assert!(!task.is_finished());
        assert_eq!(blocking.wake_one(&"stream", 7), Ok(()));
        assert_eq!(task.await.unwrap(), Ok(("stream", 7)));
        assert_empty(&blocking);
    }

    #[tokio::test]
    async fn duplicate_keys_are_registered_once_and_all_keys_are_removed() {
        let blocking = BlockingKeys::new();
        let registration = blocking.register(vec!["a", "a", "b"], None).unwrap();
        {
            let state = blocking.inner.state.lock();
            assert_eq!(state.keys["a"].len(), 1);
            assert_eq!(state.keys["b"].len(), 1);
            assert_eq!(state.waiters[&registration.id()].keys, vec!["a", "b"]);
        }
        assert_eq!(blocking.wake_one(&"b", 3), Ok(()));
        assert_eq!(registration.wait().await, Ok(("b", 3)));
        assert_eq!(blocking.wake_one(&"a", 4), Err(4));
        assert_empty(&blocking);
    }

    #[tokio::test]
    async fn wake_one_preserves_fifo_after_cancellation() {
        let blocking = BlockingKeys::new();
        let first = blocking.register(vec!["a"], None).unwrap();
        let mut second = blocking.register(vec!["a", "b"], None).unwrap();
        let third = blocking.register(vec!["a"], None).unwrap();
        assert!(second.cancel());
        assert!(!second.cancel());
        assert_eq!(second.wait().await, Err(WaitError::Closed));
        assert_eq!(blocking.wake_one(&"a", 1), Ok(()));
        assert_eq!(blocking.wake_one(&"a", 2), Ok(()));
        assert_eq!(first.wait().await, Ok(("a", 1)));
        assert_eq!(third.wait().await, Ok(("a", 2)));
        assert_empty(&blocking);
    }

    #[test]
    fn dropping_registration_removes_every_key() {
        let blocking = BlockingKeys::<_, ()>::new();
        let registration = blocking.register(vec!["a", "b", "a"], None).unwrap();
        drop(registration);
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_removes_every_key_without_a_notification() {
        let blocking = BlockingKeys::<_, ()>::new();
        assert_eq!(
            blocking.wait(vec!["a", "b"], Duration::from_secs(5)).await,
            Err(WaitError::Timeout)
        );
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_one_skips_expired_waiters_and_reports_timeout() {
        let blocking = BlockingKeys::new();
        let expired = blocking
            .register(vec!["a", "b"], Some(Instant::now()))
            .unwrap();
        let live = blocking.register(vec!["a"], None).unwrap();
        assert_eq!(blocking.wake_one(&"a", 5), Ok(()));
        assert_eq!(expired.wait().await, Err(WaitError::Timeout));
        assert_eq!(live.wait().await, Ok(("a", 5)));
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn notification_before_deadline_wins_even_when_polled_after_deadline() {
        let blocking = BlockingKeys::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let registration = blocking.register(vec!["a"], Some(deadline)).unwrap();
        assert_eq!(registration.deadline(), Some(deadline));
        assert_eq!(blocking.wake_one(&"a", 1), Ok(()));
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(registration.wait().await, Ok(("a", 1)));
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn notification_after_deadline_returns_value_and_reports_timeout() {
        let blocking = BlockingKeys::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let registration = blocking.register(vec!["a"], Some(deadline)).unwrap();
        time::advance(Duration::from_secs(2)).await;
        assert_eq!(blocking.wake_one(&"a", 1), Err(1));
        assert_eq!(registration.wait().await, Err(WaitError::Timeout));
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_all_broadcasts_once_and_cleans_up_other_keys() {
        let blocking = BlockingKeys::new();
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
        assert_eq!(blocking.inner.state.lock().waiters.len(), 2);
        assert_eq!(blocking.wake_all(&"a", 10), 1);
        assert_eq!(next.wait().await, Ok(("a", 10)));
        assert_eq!(blocking.wake_all(&"d", 11), 1);
        assert_eq!(unrelated.wait().await, Ok(("d", 11)));
        assert_empty(&blocking);
    }

    #[tokio::test]
    async fn wake_all_does_not_include_registrations_created_during_delivery() {
        struct RegisterOnClone(Arc<dyn Fn() + Send + Sync>);

        impl Clone for RegisterOnClone {
            fn clone(&self) -> Self {
                (self.0)();
                Self(self.0.clone())
            }
        }

        let blocking = BlockingKeys::new();
        let first = blocking.register(vec!["a"], None).unwrap();
        let second = blocking.register(vec!["a"], None).unwrap();
        let next = Arc::new(Mutex::new(None));
        let value = RegisterOnClone(Arc::new({
            let blocking = blocking.clone();
            let next = next.clone();
            move || {
                let mut registration = next.lock();
                if registration.is_none() {
                    *registration = Some(blocking.register(vec!["a"], None).unwrap());
                }
            }
        }));

        // Cloning the first delivery registers another waiter synchronously.
        // That waiter must not consume the broadcast already in progress.
        assert_eq!(blocking.wake_all(&"a", value), 2);
        assert!(matches!(first.wait().await, Ok(("a", _))));
        assert!(matches!(second.wait().await, Ok(("a", _))));
        assert_eq!(blocking.inner.state.lock().waiters.len(), 1);
        drop(next.lock().take());
        assert_empty(&blocking);
    }

    #[tokio::test]
    async fn wake_ready_preserves_each_requests_result_and_leaves_unready_waiters() {
        struct Request {
            after: u64,
            count: usize,
        }

        let blocking = BlockingKeys::new();
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
            let state = blocking.inner.state.lock();
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
        assert_empty(&blocking);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_ready_expires_without_resolving_and_preserves_other_deadlines() {
        let blocking = BlockingKeys::new();
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
        assert_empty(&blocking);
    }

    #[tokio::test]
    async fn aborting_wait_future_unregisters_all_keys() {
        let blocking = BlockingKeys::<_, ()>::new();
        let registration = blocking.register(vec!["a", "b"], None).unwrap();
        let task = tokio::spawn(registration.wait());
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_empty(&blocking);
    }
}
