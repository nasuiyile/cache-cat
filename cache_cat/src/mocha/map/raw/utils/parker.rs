use parking_lot_core::{DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN, park, unpark_all};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

/// Address-keyed waiting for a table's copy and promotion state.
///
/// The shared parking lot owns the waiter queues, so a table needs neither a
/// mutex nor an allocating registry of waiting threads. The counter preserves
/// the cheap no-waiter path when publishing each copied entry.
#[derive(Default)]
pub struct Parker {
    pending: AtomicUsize,
}

struct Pending<'a>(&'a AtomicUsize);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Parker {
    #[cfg(test)]
    pub(crate) fn pending_waiters(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    /// Wait until the predicate is false, acquiring the completed state.
    ///
    /// The predicate runs outside parking-lot locks and may panic. While a
    /// queue is locked we only compare sealed primitive atomic values, never
    /// invoking user code. A changed value makes validation fail and retries
    /// the predicate, even if the new value still requires waiting.
    pub fn park<T: Copy + Eq>(&self, atomic: &impl Atomic<T>, should_park: impl Fn(T) -> bool) {
        if !should_park(atomic.load(Ordering::Acquire)) {
            return;
        }

        // Registration, the caller's condition publication, and unpark's
        // pending check participate in one SeqCst order. If unpark observes
        // no waiter, the subsequent condition check observes completion.
        // Otherwise queue-locked validation closes the gap before enqueueing:
        // notify first invalidates the observation; enqueue first is woken.
        self.pending.fetch_add(1, Ordering::SeqCst);
        let _pending = Pending(&self.pending);
        let key = atomic as *const _ as usize;

        loop {
            let observed = atomic.load(Ordering::SeqCst);
            if !should_park(observed) {
                return;
            }

            // SAFETY: Map callers hold a Seize guard for this entry slot or
            // table status. Its address remains valid and is used exclusively
            // by this Parker's park/unpark pair until all waits finish. The
            // queue callbacks use only primitive atomic load/equality and
            // cannot panic, re-enter the parking lot, or invoke a predicate.
            unsafe {
                park(
                    key,
                    || atomic.load(Ordering::SeqCst) == observed,
                    || {},
                    |_, _| {},
                    DEFAULT_PARK_TOKEN,
                    None,
                );
            }
        }
    }

    /// Wake all waiters on this atomic after a SeqCst condition publication.
    pub fn unpark<T>(&self, atomic: &impl Atomic<T>) {
        if self.pending.load(Ordering::SeqCst) == 0 {
            return;
        }

        // Waiters decrement their own registration, including on predicate
        // unwind. Already-woken threads may keep this count positive briefly;
        // that only causes a conservative extra queue lookup.
        let key = atomic as *const _ as usize;
        // SAFETY: The caller keeps this table/slot alive with its Seize guard,
        // and no other synchronization primitive uses this address as a key.
        unsafe { unpark_all(key, DEFAULT_UNPARK_TOKEN) };
    }
}

mod sealed {
    use std::sync::atomic::{AtomicPtr, AtomicU8};

    pub trait Sealed {}
    impl<T> Sealed for AtomicPtr<T> {}
    impl Sealed for AtomicU8 {}
}

/// Primitive atomic states supported by the table's parking protocol.
pub trait Atomic<T>: sealed::Sealed {
    fn load(&self, ordering: Ordering) -> T;
}

impl<T> Atomic<*mut T> for AtomicPtr<T> {
    fn load(&self, ordering: Ordering) -> *mut T {
        self.load(ordering)
    }
}

impl Atomic<u8> for AtomicU8 {
    fn load(&self, ordering: Ordering) -> u8 {
        self.load(ordering)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot_core::{FilterOp, unpark_filter};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::{Duration, Instant};

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// Inspect the real queue without waking or removing its waiters.
    fn queued(atomic: &AtomicU8) -> usize {
        let mut count = 0;
        // SAFETY: Tests own this atomic address; callbacks neither panic nor
        // re-enter the parking lot. Skip leaves all queue entries untouched.
        unsafe {
            unpark_filter(
                atomic as *const _ as usize,
                |_| {
                    count += 1;
                    FilterOp::Skip
                },
                |_| DEFAULT_UNPARK_TOKEN,
            );
        }
        count
    }

    fn wait_until(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + TIMEOUT;
        while !ready() {
            assert!(Instant::now() < deadline, "waiter did not reach its queue");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn completed_condition_does_not_register_a_waiter() {
        let parker = Parker::default();
        let state = AtomicU8::new(1);
        parker.park(&state, |state| {
            assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
            state == 0
        });
        let pointer = AtomicPtr::<u8>::new(std::ptr::null_mut());
        parker.park(&pointer, |pointer| !pointer.is_null());
        assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
        assert_eq!(queued(&state), 0);
    }

    fn notification_in_predicate_race(block_on_call: usize) {
        let parker = Arc::new(Parker::default());
        let state = Arc::new(AtomicU8::new(0));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = {
            let parker = parker.clone();
            let state = state.clone();
            thread::spawn(move || {
                let calls = AtomicUsize::new(0);
                parker.park(&*state, |observed| {
                    if calls.fetch_add(1, Ordering::Relaxed) == block_on_call {
                        ready_tx.send(()).unwrap();
                        resume_rx.recv_timeout(TIMEOUT).unwrap();
                    }
                    observed == 0
                });
                done_tx.send(()).unwrap();
            })
        };
        ready_rx.recv_timeout(TIMEOUT).unwrap();
        assert_eq!(parker.pending.load(Ordering::Relaxed), block_on_call);
        assert_eq!(queued(&state), 0);
        state.store(1, Ordering::SeqCst);
        parker.unpark(&*state);
        resume_tx.send(()).unwrap();
        // Join only after a bounded completion signal, so a lost wakeup fails
        // the test instead of indefinitely blocking the whole test process.
        done_rx.recv_timeout(TIMEOUT).unwrap();
        waiter.join().unwrap();
        assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn notification_before_registration_is_not_lost() {
        notification_in_predicate_race(0);
    }

    #[test]
    fn notification_before_queue_validation_is_not_lost() {
        notification_in_predicate_race(1);
    }

    #[test]
    fn queued_waiters_wake_for_their_own_key() {
        let parker = Arc::new(Parker::default());
        let first = Arc::new(AtomicU8::new(0));
        let second = Arc::new(AtomicU8::new(0));
        let (done_tx, done_rx) = mpsc::channel();
        let mut waiters = Vec::new();
        for id in 0..5 {
            let parker = parker.clone();
            let state = if id < 3 {
                first.clone()
            } else {
                second.clone()
            };
            let done_tx = done_tx.clone();
            waiters.push(thread::spawn(move || {
                parker.park(&*state, |state| state == 0);
                done_tx.send(id).unwrap();
            }));
        }
        wait_until(|| queued(&first) == 3 && queued(&second) == 2);
        assert_eq!(parker.pending.load(Ordering::Relaxed), 5);

        first.store(1, Ordering::SeqCst);
        parker.unpark(&*first);
        for _ in 0..3 {
            assert!(done_rx.recv_timeout(TIMEOUT).unwrap() < 3);
        }
        assert_eq!(queued(&first), 0);
        assert_eq!(queued(&second), 2);
        assert_eq!(parker.pending.load(Ordering::Relaxed), 2);

        second.store(1, Ordering::SeqCst);
        parker.unpark(&*second);
        for _ in 0..2 {
            assert!(done_rx.recv_timeout(TIMEOUT).unwrap() >= 3);
        }
        for waiter in waiters {
            waiter.join().unwrap();
        }
        assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn changed_value_rechecks_predicate_before_waiting_again() {
        let parker = Arc::new(Parker::default());
        let state = Arc::new(AtomicU8::new(0));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let waiter = {
            let parker = parker.clone();
            let state = state.clone();
            thread::spawn(move || {
                let calls = AtomicUsize::new(0);
                parker.park(&*state, |observed| {
                    if calls.fetch_add(1, Ordering::Relaxed) == 1 {
                        ready_tx.send(()).unwrap();
                        resume_rx.recv_timeout(TIMEOUT).unwrap();
                    }
                    observed < 2
                });
                done_tx.send(()).unwrap();
            })
        };
        ready_rx.recv_timeout(TIMEOUT).unwrap();
        state.store(1, Ordering::SeqCst);
        parker.unpark(&*state);
        resume_tx.send(()).unwrap();
        wait_until(|| queued(&state) == 1);
        assert!(done_rx.try_recv().is_err());
        state.store(2, Ordering::SeqCst);
        parker.unpark(&*state);
        done_rx.recv_timeout(TIMEOUT).unwrap();
        waiter.join().unwrap();
        assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn predicate_panics_release_registration_without_poisoning_queue() {
        let parker = Parker::default();
        let state = AtomicU8::new(0);
        for panic_on_call in [0, 1] {
            let calls = AtomicUsize::new(0);
            let result = catch_unwind(AssertUnwindSafe(|| {
                parker.park(&state, |_| {
                    assert_ne!(calls.fetch_add(1, Ordering::Relaxed), panic_on_call);
                    true
                });
            }));
            assert!(result.is_err());
            assert_eq!(parker.pending.load(Ordering::Relaxed), 0);
            assert_eq!(queued(&state), 0);
        }
        state.store(1, Ordering::SeqCst);
        parker.park(&state, |state| state == 0);
        parker.unpark(&state);
    }
}
