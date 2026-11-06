use super::super::{Compute, HashMap, Operation, ResizeMode, raw};
use super::{Identity, map};
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashSet};
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

// Deliberately not Clone: migration must share a node until reclamation is safe,
// and must eventually destroy every owned key/value exactly once.
struct Tracked {
    logical: u64,
    slot: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        // Assert after reclamation, rather than panicking inside Drop during an
        // already-unwinding callback, which could abort the test process.
        self.drops[self.slot].fetch_add(1, Ordering::Relaxed);
    }
}
impl PartialEq for Tracked {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
impl Eq for Tracked {}
impl Hash for Tracked {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.logical.hash(state);
    }
}
impl Borrow<u64> for Tracked {
    fn borrow(&self) -> &u64 {
        &self.logical
    }
}
impl Tracked {
    fn assert_alive(&self) {
        assert_eq!(
            self.drops[self.slot].load(Ordering::Relaxed),
            0,
            "live object {} was destroyed",
            self.slot
        );
    }
}

struct DropLedger {
    drops: Arc<Vec<AtomicUsize>>,
    issued: usize,
}
impl DropLedger {
    fn new() -> Self {
        Self {
            drops: Arc::new((0..1024).map(|_| AtomicUsize::new(0)).collect()),
            issued: 0,
        }
    }
    fn make(&mut self, logical: u64) -> Tracked {
        let slot = self.issued;
        self.issued += 1;
        assert!(slot < self.drops.len());
        Tracked {
            logical,
            slot,
            drops: self.drops.clone(),
        }
    }
    fn assert_dropped_once(&self) {
        for (slot, count) in self.drops.iter().take(self.issued).enumerate() {
            assert_eq!(
                count.load(Ordering::Relaxed),
                1,
                "object {slot} must be reclaimed exactly once"
            );
        }
    }
}

fn tracked_map(capacity: usize) -> HashMap<Tracked, Tracked, Identity> {
    HashMap {
        raw: raw::HashMap::new(
            capacity,
            Identity::default(),
            seize::Collector::new(),
            ResizeMode::Incremental(1),
        ),
    }
}

#[test]
fn held_local_and_owned_guards_protect_replaced_removed_retained_and_cleared_entries() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(16);
    for key in 0..6u64 {
        map.pin().insert(ledger.make(key), ledger.make(100 + key));
    }

    let local = map.pin();
    let owned = map.pin_owned();
    let replaced = local.get_key_value(&0u64).unwrap();
    let removed = local.get_key_value(&1u64).unwrap();
    let retained_out = owned.get_key_value(&2u64).unwrap();
    let cleared = owned.get_key_value(&3u64).unwrap();

    map.pin().insert(ledger.make(0), ledger.make(200));
    map.pin().remove(&1u64);
    map.pin().retain(|key, _| key.logical != 2);
    map.pin().clear();
    assert_eq!(map.pin().len(), 0);

    for (key, value) in [replaced, removed, retained_out, cleared] {
        key.assert_alive();
        value.assert_alive();
        assert_eq!(value.logical, key.logical + 100);
    }
    drop(local);
    // The independent owned guard still protects its references after the
    // local guard leaves; neither reference is obtained by another lookup.
    retained_out.1.assert_alive();
    cleared.1.assert_alive();
    assert_eq!(cleared.1.logical, 103);
    drop(owned);
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn dropping_an_unfinished_migration_reclaims_shared_and_retired_nodes_once() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(1024);
    for key in 0..128u64 {
        map.pin().insert(ledger.make(key), ledger.make(1000 + key));
    }
    let held = map.pin_owned();
    let copied = held.get_key_value(&0u64).unwrap();
    let uncopied = held.get_key_value(&100u64).unwrap();

    map.raw.test_resize(4096, 32);
    map.pin().insert(ledger.make(0), ledger.make(2000));
    map.pin().remove(&1u64);
    let mut replacement = Some(ledger.make(2002));
    map.pin().compute(ledger.make(2), |_| {
        Operation::<_, ()>::Insert(replacement.take().unwrap())
    });
    assert!(map.raw.test_has_pending_resize());
    assert_eq!(map.pin().len(), 127);
    for (key, value) in [copied, uncopied] {
        key.assert_alive();
        value.assert_alive();
        assert_eq!(value.logical, key.logical + 1000);
    }
    drop(held);
    // Exercise Drop's unfinished-successor-chain path, without first forcing
    // migration to complete through iter/clear/retain.
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn owned_guard_can_move_threads_and_keep_retired_references_alive() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(1024);
    for key in 0..64u64 {
        map.pin().insert(ledger.make(key), ledger.make(1000 + key));
    }
    let owned = map.pin_owned();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();

    std::thread::scope(|scope| {
        let reader = scope.spawn(move || {
            let old = owned.get_key_value(&7u64).unwrap();
            ready_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            old.0.assert_alive();
            old.1.assert_alive();
            assert_eq!((old.0.logical, old.1.logical), (7, 1007));
            assert!(owned.get(&7u64).is_none());
            // The owned guard is also dropped on this other thread.
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        map.raw.test_resize(4096, 16);
        map.pin().insert(ledger.make(7), ledger.make(2007));
        map.pin().remove(&9u64);
        map.pin().clear();
        resume_tx.send(()).unwrap();
        reader.join().unwrap();
    });
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn owned_guard_iterator_moves_threads_and_covers_stable_keys_once_across_resizes() {
    let map = map(256);
    for key in 0..128u64 {
        map.pin().insert(key, key);
    }
    map.raw.test_resize(1024, 16);
    assert!(map.raw.test_has_pending_resize());
    let owned = map.pin_owned();
    let mut iterator = owned.iter();
    assert!(
        !map.raw.test_has_pending_resize(),
        "ordinary iteration completes its initial migration"
    );
    let (paused_tx, paused_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let seen = std::thread::scope(|scope| {
        // Moving this iterator also checks that an iterator borrowing an owned
        // guard is Send; the guard itself remains pinned on the parent thread.
        let reader = scope.spawn(move || {
            let mut seen = HashSet::new();
            for (key, value) in iterator.by_ref().take(32) {
                assert_eq!(key, value);
                assert!(seen.insert(*key));
            }
            paused_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            for (key, value) in iterator {
                assert_eq!(key, value);
                assert!(seen.insert(*key), "iterator emitted a duplicated key");
            }
            seen
        });
        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        map.raw.test_resize(2048, 64);
        map.raw.test_finish_resize();
        resume_tx.send(()).unwrap();
        for key in 0..128u64 {
            map.pin().insert(key, key);
        }
        for key in 512..1024u64 {
            map.pin().insert(key, key);
        }
        reader.join().unwrap()
    });
    assert_eq!(seen, (0..128u64).collect());
    drop(owned);
    for key in 0..128u64 {
        assert_eq!(map.pin().get(&key), Some(&key));
    }
}

#[test]
fn compute_callback_panics_leave_existing_and_missing_keys_unchanged() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(1024);
    map.pin().insert(ledger.make(0), ledger.make(100));
    map.raw.test_resize(4096, 1);

    for logical in [0u64, 99] {
        let input = ledger.make(logical);
        let result = catch_unwind(AssertUnwindSafe(|| {
            map.pin()
                .compute(input, |current| -> Operation<Tracked, ()> {
                    assert_eq!(current.is_some(), logical == 0);
                    panic!("callback failure before publishing a transition");
                });
        }));
        assert!(result.is_err());
        assert_eq!(map.pin().len(), 1);
        assert_eq!(map.pin().get(&0u64).unwrap().logical, 100);
        assert!(map.pin().get(&99u64).is_none());
    }
    let mut value = Some(ledger.make(200));
    map.pin().compute(ledger.make(0), |_| {
        Operation::<_, ()>::Insert(value.take().unwrap())
    });
    map.raw.test_finish_resize();
    assert_eq!(map.pin().get(&0u64).unwrap().logical, 200);
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn compute_retry_callback_panic_reclaims_the_unpublished_key_and_value() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(16);
    map.pin().insert(ledger.make(0), ledger.make(100));
    let input = ledger.make(0);
    let computed = ledger.make(999);
    let computed_slot = computed.slot;
    let replacement_key = ledger.make(0);
    let replacement_value = ledger.make(200);
    let (observed_tx, observed_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();

    let (panicked, calls) = std::thread::scope(|scope| {
        let map_ref = &map;
        let computer = scope.spawn(move || {
            let mut calls = 0;
            let mut computed = Some(computed);
            let result = catch_unwind(AssertUnwindSafe(|| {
                map_ref
                    .pin()
                    .compute(input, |current| -> Operation<Tracked, ()> {
                        calls += 1;
                        let current = current.expect("the key remains present").1;
                        if calls == 1 {
                            assert_eq!(current.logical, 100);
                            // Test coordination only: let the competing writer
                            // replace this exact input before the first CAS.
                            observed_tx.send(()).unwrap();
                            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                            Operation::Insert(computed.take().unwrap())
                        } else {
                            assert_eq!(calls, 2);
                            assert_eq!(current.logical, 200);
                            panic!("callback retry fails after a lost CAS");
                        }
                    });
            }));
            (result.is_err(), calls)
        });
        observed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        map.pin().insert(replacement_key, replacement_value);
        resume_tx.send(()).unwrap();
        computer.join().unwrap()
    });
    assert!(panicked);
    assert_eq!(calls, 2, "the first computed transition must lose its CAS");
    assert_eq!(map.pin().get(&0u64).unwrap().logical, 200);
    assert_eq!(map.pin().len(), 1);
    map.pin().insert(ledger.make(1), ledger.make(201));
    assert_eq!(map.pin().get(&1u64).unwrap().logical, 201);
    drop(map);
    assert_eq!(
        ledger.drops[computed_slot].load(Ordering::Relaxed),
        1,
        "the discarded computed value must not leak either"
    );
    ledger.assert_dropped_once();
}

struct PanicOnCaptureDrop(Arc<AtomicUsize>);

impl PanicOnCaptureDrop {
    fn observe(&self) {
        assert_eq!(self.0.load(Ordering::Relaxed), 0);
    }
}
impl Drop for PanicOnCaptureDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic!("callback capture destructor fails after publication");
    }
}

#[test]
fn callback_capture_drop_panic_does_not_reclaim_a_published_insert_or_update() {
    for existing in [false, true] {
        let mut ledger = DropLedger::new();
        let map = tracked_map(16);
        if existing {
            map.pin().insert(ledger.make(0), ledger.make(100));
        }
        let key = ledger.make(0);
        let value = ledger.make(200);
        let published_slots = [key.slot, value.slot];
        let capture_drops = Arc::new(AtomicUsize::new(0));
        let capture = PanicOnCaptureDrop(capture_drops.clone());
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut value = Some(value);
            map.pin().compute(key, move |old| {
                capture.observe();
                assert_eq!(old.is_some(), existing);
                Operation::<Tracked, ()>::Insert(value.take().unwrap())
            });
        }));
        assert!(result.is_err());
        assert_eq!(capture_drops.load(Ordering::Relaxed), 1);
        // A naive unwinding guard can accidentally free an already-published
        // node. Detect destruction before dereferencing the map's pointer; in
        // that failure case avoid a second free while unwinding the test.
        if published_slots
            .iter()
            .any(|&slot| ledger.drops[slot].load(Ordering::Relaxed) != 0)
        {
            std::mem::forget(map);
            panic!("published key/value destroyed by capture-drop panic; existing={existing}");
        }
        assert_eq!(map.pin().len(), 1);
        let pin = map.pin();
        let (published_key, published_value) = pin.get_key_value(&0u64).unwrap();
        published_key.assert_alive();
        published_value.assert_alive();
        assert_eq!(published_value.logical, 200);
        drop(pin);
        drop(map);
        ledger.assert_dropped_once();
    }
}

#[derive(Default)]
struct HashPanicControl {
    armed: AtomicBool,
    panics: AtomicUsize,
}

struct HashPanicKey {
    tracked: Tracked,
    control: Option<Arc<HashPanicControl>>,
}
impl PartialEq for HashPanicKey {
    fn eq(&self, other: &Self) -> bool {
        self.tracked == other.tracked
    }
}
impl Eq for HashPanicKey {}
impl Borrow<u64> for HashPanicKey {
    fn borrow(&self) -> &u64 {
        &self.tracked.logical
    }
}
impl Hash for HashPanicKey {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        if let Some(control) = &self.control {
            if control.armed.load(Ordering::Acquire) {
                control.panics.fetch_add(1, Ordering::Relaxed);
                panic!("hashing the competing entry after a lost insertion CAS");
            }
        }
        self.tracked.hash(hasher);
    }
}

#[test]
fn hash_panic_after_losing_empty_slot_cas_reclaims_initialized_candidate() {
    let mut ledger = DropLedger::new();
    // The root must already exist: otherwise the initial None callback runs
    // before locating an empty slot, and no insertion CAS race is forced.
    let map = HashMap::with_capacity_and_hasher(16, Identity::default());
    let candidate_key = HashPanicKey {
        tracked: ledger.make(0),
        control: None,
    };
    let candidate_value = ledger.make(100);
    let control = Arc::new(HashPanicControl::default());
    let competing_key = HashPanicKey {
        tracked: ledger.make(0),
        control: Some(control.clone()),
    };
    let competing_value = ledger.make(200);
    let (observed_tx, observed_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let (panicked, calls) = std::thread::scope(|scope| {
        let map_ref = &map;
        let computer = scope.spawn(move || {
            let mut calls = 0;
            let mut value = Some(candidate_value);
            let result = catch_unwind(AssertUnwindSafe(|| {
                map_ref.pin().compute(candidate_key, |entry| {
                    calls += 1;
                    assert!(entry.is_none());
                    observed_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Operation::<Tracked, ()>::Insert(value.take().unwrap())
                });
            }));
            (result.is_err(), calls)
        });
        observed_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        map.pin().insert(competing_key, competing_value);
        control.armed.store(true, Ordering::Release);
        resume_tx.send(()).unwrap();
        computer.join().unwrap()
    });
    control.armed.store(false, Ordering::Release);
    assert!(panicked);
    assert_eq!(calls, 1, "panic must occur before another callback");
    assert_eq!(control.panics.load(Ordering::Relaxed), 1);
    assert!(
        !map.raw.test_has_pending_resize(),
        "no panic occurs during COPYING"
    );
    assert_eq!(map.pin().len(), 1);
    assert_eq!(map.pin().get(&0u64).unwrap().logical, 200);
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn aborted_compute_returns_ownership_and_remove_keeps_guarded_references_valid() {
    let mut ledger = DropLedger::new();
    let map = tracked_map(16);
    map.pin().insert(ledger.make(0), ledger.make(100));
    let pin = map.pin();
    let mut owned_abort = Some(ledger.make(999));
    let result = pin.compute(ledger.make(0), |_| {
        Operation::<Tracked, Tracked>::Abort(owned_abort.take().unwrap())
    });
    let Compute::Aborted(aborted) = result else {
        panic!("expected abort payload");
    };
    aborted.assert_alive();
    assert_eq!(aborted.logical, 999);
    assert_eq!(pin.get(&0u64).unwrap().logical, 100);

    let removed = pin.compute(ledger.make(0), |_| Operation::<Tracked, ()>::Remove);
    let Compute::Removed(key, value) = removed else {
        panic!("expected removed entry");
    };
    assert_eq!(pin.len(), 0);
    key.assert_alive();
    value.assert_alive();
    assert_eq!((key.logical, value.logical), (0, 100));
    drop(aborted);
    drop(pin);
    drop(map);
    ledger.assert_dropped_once();
}

#[test]
fn retain_callback_panic_preserves_decisions_already_committed_and_all_other_keys() {
    let map = map(128);
    for key in 0..32u64 {
        map.pin().insert(key, key);
    }
    map.raw.test_resize(512, 3);
    let mut removed = BTreeSet::new();
    let mut calls = 0;
    let result = catch_unwind(AssertUnwindSafe(|| {
        map.pin().retain(|key, value| {
            assert_eq!(key, value);
            calls += 1;
            if calls == 9 {
                panic!("retain predicate failure");
            }
            let keep = key % 2 == 0;
            if !keep {
                removed.insert(*key);
            }
            keep
        });
    }));
    assert!(result.is_err());
    assert_eq!(calls, 9);
    let expected: BTreeSet<_> = (0..32u64).filter(|key| !removed.contains(key)).collect();
    assert_eq!(map.pin().len(), expected.len());
    assert_eq!(
        map.pin()
            .iter()
            .map(|(key, _)| *key)
            .collect::<BTreeSet<_>>(),
        expected
    );
    map.pin().retain(|key, _| key % 2 == 0);
    assert_eq!(map.pin().len(), 16);
    map.pin().clear();
    map.pin().insert(999, 999);
    assert_eq!(map.pin().get(&999), Some(&999));
}

#[test]
fn scan_callback_panic_can_restart_without_losing_keys_or_finishing_migration() {
    let map = map(1024);
    for key in 0..64u64 {
        map.pin().insert(key, key);
    }
    map.raw.test_resize(4096, 8);
    let mut calls = 0;
    let result = catch_unwind(AssertUnwindSafe(|| {
        map.pin().scan(0, usize::MAX, |key, value| {
            assert_eq!(key, value);
            calls += 1;
            if calls == 3 {
                panic!("scan visitor failure");
            }
        });
    }));
    assert!(result.is_err());
    assert_eq!(calls, 3);
    assert!(map.raw.test_has_pending_resize());
    assert_eq!(map.pin().len(), 64);
    map.pin().remove(&63);
    map.pin().insert(4096, 4096);
    let seen: BTreeSet<_> = super::full_scan(&map, 3).into_iter().collect();
    assert_eq!(seen, (0..63u64).chain([4096]).collect());
    assert!(map.raw.test_has_pending_resize());
    map.raw.test_finish_resize();
    assert_eq!(map.pin().len(), 64);
}
