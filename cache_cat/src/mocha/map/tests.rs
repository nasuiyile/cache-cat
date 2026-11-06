use super::{HashMap, Operation, ResizeMode, raw};
use std::collections::{BTreeSet, HashSet};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

mod concurrency;
mod lifecycle;
mod model;
mod scan;

#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        let mut word = [0u8; 8];
        for (destination, source) in word.iter_mut().zip(bytes) {
            *destination = *source;
        }
        self.0 = u64::from_le_bytes(word);
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type Identity = BuildHasherDefault<IdentityHasher>;
type TestMap<V = u64> = HashMap<u64, V, Identity>;

fn map(capacity: usize) -> TestMap {
    HashMap {
        raw: raw::HashMap::new(
            capacity,
            Identity::default(),
            seize::Collector::new(),
            ResizeMode::Incremental(1),
        ),
    }
}

fn full_scan(map: &TestMap, count: usize) -> Vec<u64> {
    let mut cursor = 0;
    let mut keys = Vec::new();
    for _ in 0..100_000 {
        cursor = map.pin().scan(cursor, count, |key, value| {
            assert_eq!(key, value);
            keys.push(*key);
        });
        if cursor == 0 {
            return keys;
        }
    }
    panic!("bounded map did not complete its scan");
}

#[test]
fn empty_unallocated_preallocated_and_cleared_maps_finish_immediately() {
    assert_eq!(map(0).pin().scan(0, 1, |_, _| panic!("empty")), 0);
    for capacity in [0, 1024] {
        let map = map(capacity);
        assert_eq!(map.pin().scan(0, 1, |_, _| panic!("preallocated")), 0);
        map.pin().insert(1, 1);
        map.pin().remove(&1);
        assert_eq!(map.pin().scan(0, 1, |_, _| panic!("deleted")), 0);
        map.pin().insert(2, 2);
        map.pin().clear();
        assert_eq!(map.pin().scan(0, 1, |_, _| panic!("cleared")), 0);
    }
}

#[test]
fn zero_length_hint_cannot_omit_a_stable_key() {
    let map = map(4096);
    map.pin().insert(1, 1);
    // Model a non-atomic sum of the independent insertion/deletion shards.
    // This must never turn a scan of an existing key into an empty full scan.
    map.raw.test_adjust_count(-1);
    assert_eq!(map.pin().len(), 0);
    let mut page = Vec::new();
    let cursor = map.pin().scan(0, 1, |key, _| page.push(*key));
    assert_ne!(cursor, 0);
    assert!(page.is_empty());
    assert_eq!(full_scan(&map, 1), [1]);
    map.raw.test_adjust_count(1);
}

#[test]
fn empty_scan_ignores_deleted_copies_in_pending_resize() {
    let map = map(512);
    map.pin().insert(0, 0);
    map.raw.test_resize(2048, 1);
    map.pin().remove(&0);
    assert_eq!(map.pin().len(), 0);
    assert!(map.raw.test_has_pending_resize());
    assert!(map.raw.test_root_contains(&0));
    assert_eq!(map.pin().scan(0, 1, |_, _| panic!("deleted copy")), 0);
    assert!(map.raw.test_has_pending_resize());
}

#[test]
fn steady_scans_visit_each_key_once_for_varied_count_and_capacity() {
    for size in [1u64, 3, 8, 31, 128, 1000] {
        let map = map(size as usize);
        for key in 0..size {
            map.pin().insert(key, key);
        }
        map.raw.test_finish_resize();
        for count in [1, 3, 10, 1000, usize::MAX] {
            let keys = full_scan(&map, count);
            assert_eq!(keys.len(), size as usize);
            assert_eq!(
                keys.into_iter().collect::<BTreeSet<_>>(),
                (0..size).collect()
            );
        }
    }
}

#[test]
fn sparse_scan_can_return_an_empty_page_with_a_nonzero_cursor() {
    let map = map(4096);
    map.pin().insert(1, 1);
    let mut page = Vec::new();
    let cursor = map.pin().scan(0, 1, |key, _| page.push(*key));
    assert!(page.is_empty());
    assert_ne!(cursor, 0);
    assert_eq!(full_scan(&map, 1), [1]);
}

#[derive(Eq, PartialEq, Debug)]
struct CollisionKey(u64);

impl Hash for CollisionKey {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        hasher.write_u64(0);
    }
}

#[test]
fn count_never_splits_a_logical_bucket_or_duplicates_small_table_probes() {
    for size in [1u64, 7, 24] {
        let map = HashMap::with_capacity_and_hasher(128, Identity::default());
        for key in 0..size {
            map.pin().insert(CollisionKey(key), key);
        }
        let mut page = Vec::new();
        let cursor = map.pin().scan(0, 1, |key, value| {
            assert_eq!(key.0, *value);
            page.push(key.0);
        });
        assert_ne!(cursor, 0);
        assert_eq!(page.len(), size as usize);
        assert_eq!(
            page.into_iter().collect::<BTreeSet<_>>(),
            (0..size).collect()
        );
    }
    // Eight slots have a probe limit larger than their capacity. A full
    // logical bucket must still be emitted only once.
    let map = HashMap::with_capacity_and_hasher(1, Identity::default());
    for key in 0..8 {
        map.pin().insert(CollisionKey(key), key);
    }
    let mut page = Vec::new();
    map.pin().scan(0, 1, |key, _| page.push(key.0));
    assert_eq!(page.len(), 8);
    assert_eq!(page.into_iter().collect::<HashSet<_>>().len(), 8);
}

#[test]
fn deleting_a_probe_predecessor_does_not_hide_displaced_keys() {
    let map = HashMap::with_capacity_and_hasher(128, Identity::default());
    for key in 0..24 {
        map.pin().insert(CollisionKey(key), key);
    }
    for key in (0..24).step_by(2) {
        map.pin().remove(&CollisionKey(key));
    }
    let mut found = BTreeSet::new();
    map.pin().scan(0, 1, |key, _| {
        found.insert(key.0);
    });
    assert_eq!(found, (1..24).step_by(2).collect());
}

#[test]
fn growth_shrink_and_same_size_rehash_between_pages_preserve_stable_keys() {
    for next_capacity in [128, 512, 2048] {
        let map = map(256);
        for key in 0..64 {
            map.pin().insert(key, key);
        }
        assert_eq!(map.raw.test_root_capacity(), 512);
        let mut seen = BTreeSet::new();
        let mut cursor = map.pin().scan(0, 1, |key, _| {
            seen.insert(*key);
        });
        assert_ne!(cursor, 0);
        map.raw.test_resize(next_capacity, 7);
        assert!(map.raw.test_has_pending_resize());
        for page in 0..10_000 {
            cursor = map.pin().scan(cursor, 3, |key, _| {
                seen.insert(*key);
            });
            if page == 2 {
                map.raw.test_finish_resize();
                assert_eq!(map.raw.test_root_capacity(), next_capacity);
            }
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(cursor, 0);
        assert_eq!(seen, (0..64).collect());
    }
}

#[test]
fn scans_cover_incremental_successor_entries_without_finishing_rehash() {
    let map = map(2048);
    for key in 0..32 {
        map.pin().insert(key, key);
    }
    assert_eq!(map.raw.test_root_capacity(), 4096);
    // All 61 quadratic probe slots for home zero lie in this prefix. Copying
    // them forces a new colliding key into the successor while leaving the
    // rest of the source migration unfinished.
    map.raw.test_resize(8192, 1900);
    map.pin().insert(4096, 4096);
    assert!(!map.raw.test_root_contains(&4096));
    assert_eq!(map.pin().get(&4096), Some(&4096));
    let keys = full_scan(&map, 1).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(keys, (0..32).chain([4096]).collect());
    assert!(
        map.raw.test_has_pending_resize(),
        "SCAN must not finish a rehash"
    );
}

#[test]
fn default_map_writes_leave_large_incremental_rehash_unfinished() {
    // Use the production constructor rather than this module's fixture with
    // a one-slot copy chunk. Ordinary updates must retain the default bounded
    // migration work instead of completing an entire large rehash.
    let map = HashMap::with_capacity_and_hasher(2048, Identity::default());
    map.pin().insert(0u64, 0u64);
    assert_eq!(map.raw.test_root_capacity(), 4096);
    map.raw.test_resize(8192, 1);

    assert_eq!(map.pin().insert(0, 1), Some(&0));
    assert_eq!(map.raw.test_root_capacity(), 4096);
    assert!(map.raw.test_has_pending_resize());

    map.pin().compute(0, |entry| {
        assert_eq!(entry.map(|(_, value)| *value), Some(1));
        Operation::<_, ()>::Insert(2)
    });
    assert_eq!(map.pin().get(&0), Some(&2));
    assert_eq!(map.raw.test_root_capacity(), 4096);
    assert!(map.raw.test_has_pending_resize());

    assert_eq!(map.pin().remove(&0), Some(&2));
    assert_eq!(map.raw.test_root_capacity(), 4096);
    assert!(map.raw.test_has_pending_resize());
}

#[test]
fn copied_entries_deleted_before_scan_are_not_resurrected() {
    let map = map(512);
    for key in 0..32 {
        map.pin().insert(key, key);
    }
    map.raw.test_resize(2048, 4);
    map.pin().remove(&0);
    map.pin().remove(&1);
    assert!(map.raw.test_has_pending_resize());
    assert!(
        map.raw.test_root_contains(&0),
        "fixture must retain an obsolete copy"
    );
    let keys = full_scan(&map, 1).into_iter().collect::<BTreeSet<_>>();
    assert_eq!(keys, (2..32).collect());
    assert!(map.raw.test_has_pending_resize());
}

#[test]
fn copied_entries_use_the_current_value_after_replacement() {
    let map = map(512);
    map.pin().insert(0, 10);
    map.raw.test_resize(2048, 1);
    map.pin().insert(0, 20);
    let mut values = Vec::new();
    map.pin().scan(0, 1, |key, value| {
        assert_eq!(*key, 0);
        values.push(*value);
    });
    assert!(!values.is_empty());
    assert!(values.iter().all(|value| *value == 20));
}

#[test]
fn visitor_resizing_cannot_extend_the_captured_frontier() {
    let map = map(128);
    for key in 0..32 {
        map.pin().insert(key, key);
    }
    let mut visited = BTreeSet::new();
    let mut resized = false;
    let cursor = map.pin().scan(0, usize::MAX, |key, _| {
        visited.insert(*key);
        if !resized {
            resized = true;
            map.raw.test_resize(1024, 0);
            map.raw.test_finish_resize();
            map.raw.test_resize(4096, 0);
            map.raw.test_finish_resize();
        }
    });
    assert_eq!(cursor, 0);
    assert_eq!(visited, (0..32).collect());
}

#[test]
fn arbitrary_cursor_bits_are_normalized_and_zero_count_is_safe() {
    let map = map(32);
    map.pin().insert(7, 7);
    for cursor in [u64::MAX, 1 << 63, u64::MAX - 3] {
        let next = map
            .pin()
            .scan(cursor, 0, |key, value| assert_eq!(key, value));
        assert!(next < map.raw.test_root_capacity() as u64);
    }
}

#[test]
fn concurrent_readers_and_one_writer_keep_stable_scan_coverage() {
    let map = Arc::new(HashMap::<u64, (u64, u64)>::new());
    for key in 0..128 {
        map.pin().insert(key, (key, 0));
    }
    let start = Barrier::new(4);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for generation in 1..=4000 {
                let key = generation % 128;
                map.pin().insert(key, (key, generation));
                let transient = generation + 128;
                map.pin().insert(transient, (transient, generation));
                if generation > 256 {
                    map.pin().remove(&(transient - 256));
                }
                if generation % 64 == 0 {
                    std::thread::yield_now();
                }
            }
        });
        for reader in 0..3 {
            let map = &map;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for round in 0..12 {
                    let mut cursor = 0;
                    let mut seen = BTreeSet::new();
                    for page in 0..100_000 {
                        let pin = map.pin();
                        cursor = pin.scan(cursor, 1 + (round + reader) % 13, |key, value| {
                            assert_eq!(*key, value.0, "torn key/value publication");
                            seen.insert(*key);
                        });
                        let key = (page % 128) as u64;
                        assert_eq!(pin.get(&key).unwrap().0, key);
                        if cursor == 0 {
                            break;
                        }
                    }
                    assert_eq!(cursor, 0);
                    assert!((0..128).all(|key| seen.contains(&key)));
                }
            });
        }
    });
}

#[test]
fn held_guards_keep_replaced_removed_and_cleared_values_alive() {
    struct Value(Arc<AtomicUsize>);
    impl Drop for Value {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    {
        let map = HashMap::new();
        let guard = map.pin_owned();
        guard.insert(0, Value(dropped.clone()));
        let value = guard.get(&0).unwrap();
        guard.compute(0, |_| Operation::<_, ()>::Insert(Value(dropped.clone())));
        guard.remove(&0);
        for key in 1..65 {
            guard.insert(key, Value(dropped.clone()));
        }
        guard.clear();
        assert!(Arc::ptr_eq(&value.0, &dropped));
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }
    assert_eq!(dropped.load(Ordering::Relaxed), 66);
}

#[test]
fn paused_copy_wakes_all_updaters_removers_and_computes_without_losing_scan_keys() {
    // insert_copy hashes the source key after setting COPYING and before
    // publishing COPIED. Only that first armed hash blocks; point lookups and
    // SCAN can continue hashing the same key while the copier is suspended.
    struct CopyGate {
        armed: AtomicBool,
        entered: crossbeam_channel::Sender<()>,
        resume: crossbeam_channel::Receiver<()>,
    }

    #[derive(Clone)]
    struct Key {
        id: u64,
        gate: Arc<CopyGate>,
    }

    impl PartialEq for Key {
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }

    impl Eq for Key {}

    impl Hash for Key {
        fn hash<H: Hasher>(&self, hasher: &mut H) {
            hasher.write_u64(self.id);
            if self.id == 0 && self.gate.armed.swap(false, Ordering::AcqRel) {
                self.gate.entered.send(()).unwrap();
                self.gate
                    .resume
                    .recv_timeout(Duration::from_secs(15))
                    .expect("test must release the paused copier");
            }
        }
    }

    // Each successful replacement installs a distinct token. The observed
    // old values, removals and final value must admit one serial execution;
    // this checks returned values without assuming which waiter wakes first.
    fn serializes(
        current: Option<u64>,
        final_value: Option<u64>,
        transitions: &[(Option<u64>, Option<u64>)],
        remaining: usize,
    ) -> bool {
        if remaining == 0 {
            return current == final_value;
        }
        transitions.iter().enumerate().any(|(index, &(old, new))| {
            remaining & (1 << index) != 0
                && old == current
                && serializes(new, final_value, transitions, remaining & !(1 << index))
        })
    }

    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (resume_tx, resume_rx) = crossbeam_channel::bounded(1);
    let gate = Arc::new(CopyGate {
        armed: AtomicBool::new(false),
        entered: entered_tx,
        resume: resume_rx,
    });
    let map = Arc::new(HashMap {
        raw: raw::HashMap::new(
            512,
            Identity::default(),
            seize::Collector::new(),
            ResizeMode::Incremental(1),
        ),
    });
    let target = Key {
        id: 0,
        gate: gate.clone(),
    };
    map.pin().insert(target.clone(), 100);
    for id in 1..=32 {
        map.pin().insert(
            Key {
                id,
                gate: gate.clone(),
            },
            id,
        );
    }

    gate.armed.store(true, Ordering::Release);
    let (copier_done_tx, copier_done_rx) = crossbeam_channel::bounded(1);
    let copying_map = map.clone();
    let copier = std::thread::spawn(move || {
        copying_map.raw.test_resize(2048, 1);
        copier_done_tx.send(()).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(map.pin().get(&target), Some(&100));
    let mut seen = BTreeSet::new();
    let mut cursor = map.pin().scan(0, 1, |key, _| {
        seen.insert(key.id);
    });
    assert_ne!(cursor, 0);

    const WORKERS: usize = 6;
    let (done_tx, done_rx) = crossbeam_channel::bounded(WORKERS);
    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let map = map.clone();
        let key = target.clone();
        let done = done_tx.clone();
        workers.push(std::thread::spawn(move || {
            let pin = map.pin();
            let token = 1_000 + worker as u64;
            let transition = match worker % 3 {
                0 => (pin.insert(key, token).copied(), Some(token)),
                1 => (pin.remove(&key).copied(), None),
                _ => match pin.compute(key, |_| Operation::<_, ()>::Insert(token)) {
                    super::Compute::Inserted(_, _) => (None, Some(token)),
                    super::Compute::Updated { old: (_, old), .. } => (Some(*old), Some(token)),
                    _ => panic!("unexpected compute transition"),
                },
            };
            done.send(transition).unwrap();
        }));
    }
    drop(done_tx);

    let deadline = Instant::now() + Duration::from_secs(10);
    while map.raw.test_pending_copy_waiters() != WORKERS && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let waiting = map.raw.test_pending_copy_waiters();
    // Always release before asserting, so a failed registration check does
    // not leave the copier suspended. Timeout receives precede joins; a lost
    // wakeup fails the test instead of indefinitely joining blocked writers.
    resume_tx.send(()).unwrap();
    assert_eq!(
        waiting, WORKERS,
        "all operations must exercise the park path"
    );
    copier_done_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    copier.join().unwrap();
    let transitions: Vec<_> = (0..WORKERS)
        .map(|_| {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("copy completion must wake every writer")
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(map.raw.test_pending_copy_waiters(), 0);
    assert!(
        serializes(
            Some(100),
            map.pin().get(&target).copied(),
            &transitions,
            (1 << WORKERS) - 1,
        ),
        "returned old values must match a serial execution: {transitions:?}"
    );

    for _ in 0..16_384 {
        cursor = map.pin().scan(cursor, 3, |key, value| {
            if key.id != 0 {
                assert_eq!(key.id, *value);
            }
            seen.insert(key.id);
        });
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(cursor, 0);
    assert!((1..=32).all(|id| seen.contains(&id)));
}

#[test]
fn competing_helpers_observe_one_published_resize_successor() {
    const HELPERS: usize = 8;
    let map = Arc::new(map(1024));
    for key in 0..32 {
        map.pin().insert(key, key);
    }
    let start = Arc::new(Barrier::new(HELPERS + 1));
    let (done_tx, done_rx) = crossbeam_channel::bounded(HELPERS);
    let mut helpers = Vec::new();
    for _ in 0..HELPERS {
        let map = map.clone();
        let start = start.clone();
        let done = done_tx.clone();
        helpers.push(std::thread::spawn(move || {
            start.wait();
            done.send(map.raw.test_allocate_next_address(4096)).unwrap();
        }));
    }
    drop(done_tx);
    start.wait();
    let addresses: BTreeSet<_> = (0..HELPERS)
        .map(|_| {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("all allocation helpers must finish")
        })
        .collect();
    for helper in helpers {
        helper.join().unwrap();
    }
    assert_eq!(addresses.len(), 1, "helpers must share one successor");
    assert!(map.raw.test_has_pending_resize());
    assert_eq!(
        full_scan(&map, 3).into_iter().collect::<BTreeSet<_>>(),
        (0..32).collect()
    );
    map.raw.test_finish_resize();
    assert_eq!(map.raw.test_root_capacity(), 4096);
}
