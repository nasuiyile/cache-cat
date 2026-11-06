//! Compare Mocha's concurrent map with papaya 0.2.4.
//!
//! `cargo run -p cache_cat --release --example mocha_map_bench -- --samples 7`
//! Optional: --cardinalities 64,1024,65536 --widths 24,96 --min-ms 10
//! --operations get-hit,get-miss,update,compute,delete,insert-reserved,
//! insert-growing,churn,iterate,read-write,scan-10,scan-100 --memory --details
//!
//! Both maps use the SAME cloned std RandomState per sample, shared Bytes keys,
//! and a 56-byte inline payload (the x64 size of Mocha's Entry<MyValue>).
//! Timing includes one pin per point operation, as in Mocha, and excludes key
//! generation, table setup, destruction and thread startup. One warmup sample
//! is discarded; paired runs alternate order. Each sequential sample accumulates
//! at least --min-ms of measured work. Reads, updates and iteration reuse warmed
//! tables; churn alternates disjoint key sets. Delete/insert batches use fresh
//! tables prepared outside the timer. read-write uses one writer and 3 readers,
//! 100,000 operations each per round, starting at a barrier; rounds accumulate
//! --min-ms and report aggregate ns/op. --details adds p10/median/p90 of paired
//! per-sample candidate/baseline ratios (a noise indicator, not a confidence interval).
//! scan rows report ns per visited key for a complete traversal with the indicated
//! COUNT; papaya has no cursor scan API, so its comparison columns are empty.
//!
//! Memory mode counts requested allocator bytes, including reclamation metadata,
//! retired entries and table allocations; it is NOT an RSS measurement. Shared
//! key backing storage is prepared beforehand and excluded equally. Each row
//! includes the inline map object, peak construction/churn allocation, and live
//! bytes after dropping every pin and flushing the retirement batch. Allocation
//! counters are disabled in timing mode. This is a container microbenchmark;
//! TTL, command parsing, RESP, Raft and network latency are excluded.

#[allow(dead_code, unused_imports)]
#[path = "../src/mocha/map/mod.rs"]
mod map;

use bytes::Bytes;
use seize::Guard;
use std::alloc::{GlobalAlloc, Layout};
use std::collections::hash_map::RandomState;
use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::time::{Duration, Instant};

struct CountingAllocator;
static TRACK: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

fn account(delta: isize) {
    if TRACK.load(Ordering::Relaxed) {
        let live = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

// SAFETY: All requests are forwarded unchanged to the same MiMalloc allocator.
// Accounting only observes sizes and never accesses allocations or allocates.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            account(layout.size() as isize);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !ptr.is_null() {
            account(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
        account(-(layout.size() as isize));
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let result = unsafe { mimalloc::MiMalloc.realloc(ptr, old, size) };
        if !result.is_null() {
            account(size as isize - old.size() as isize);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

type Payload = [u64; 7];
type Original = papaya::HashMap<Bytes, Payload>;
type Scannable = map::HashMap<Bytes, Payload>;

trait BenchMap: Sync + Sized {
    fn empty(capacity: usize, hasher: RandomState) -> Self;
    fn read(&self, key: &Bytes) -> Option<u64>;
    fn write(&self, key: Bytes, value: u64);
    fn compute(&self, key: Bytes);
    fn remove(&self, key: &Bytes);
    fn visit(&self) -> usize;
    fn quiesce(&self);
    fn with_reader<T>(&self, work: impl FnOnce() -> T) -> T;
}

macro_rules! bench_map {
    ($ty:ty, $operation:path) => {
        impl BenchMap for $ty {
            fn empty(capacity: usize, hasher: RandomState) -> Self {
                Self::with_capacity_and_hasher(capacity, hasher)
            }
            #[inline]
            fn read(&self, key: &Bytes) -> Option<u64> {
                self.pin().get(key).map(|value| value[0])
            }
            #[inline]
            fn write(&self, key: Bytes, value: u64) {
                black_box(self.pin().insert(key, [value; 7]));
            }
            #[inline]
            fn compute(&self, key: Bytes) {
                use $operation as Operation;
                black_box(self.pin().compute(key, |old| {
                    Operation::<Payload, ()>::Insert([old.map_or(0, |(_, v)| v[0]) + 1; 7])
                }));
            }
            #[inline]
            fn remove(&self, key: &Bytes) {
                black_box(self.pin().remove(key));
            }
            fn visit(&self) -> usize {
                self.pin().iter().fold(0usize, |sum, (key, value)| {
                    black_box(value);
                    sum.wrapping_add(black_box(key).len())
                })
            }
            fn quiesce(&self) {
                self.guard().flush();
            }
            fn with_reader<T>(&self, work: impl FnOnce() -> T) -> T {
                let reader = self.guard();
                let result = work();
                black_box(&reader);
                result
            }
        }
    };
}
bench_map!(Original, papaya::Operation);
bench_map!(Scannable, map::Operation);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Hit,
    Miss,
    Update,
    Compute,
    Delete,
    InsertReserved,
    InsertGrowing,
    Churn,
    Iterate,
    ReadWrite,
    Scan10,
    Scan100,
}

impl Operation {
    const ALL: [Self; 12] = [
        Self::Hit,
        Self::Miss,
        Self::Update,
        Self::Compute,
        Self::Delete,
        Self::InsertReserved,
        Self::InsertGrowing,
        Self::Churn,
        Self::Iterate,
        Self::ReadWrite,
        Self::Scan10,
        Self::Scan100,
    ];
    fn name(self) -> &'static str {
        match self {
            Self::Hit => "get-hit",
            Self::Miss => "get-miss",
            Self::Update => "update",
            Self::Compute => "compute",
            Self::Delete => "delete",
            Self::InsertReserved => "insert-reserved",
            Self::InsertGrowing => "insert-growing",
            Self::Churn => "churn",
            Self::Iterate => "iterate",
            Self::ReadWrite => "read-write",
            Self::Scan10 => "scan-10",
            Self::Scan100 => "scan-100",
        }
    }
}

struct Config {
    samples: usize,
    counts: Vec<usize>,
    widths: Vec<usize>,
    operations: Vec<Operation>,
    min_time: Duration,
    memory: bool,
    details: bool,
}

impl Config {
    fn parse() -> Self {
        let mut config = Self {
            samples: 7,
            counts: vec![64, 1024, 65_536],
            widths: vec![24, 96],
            operations: Operation::ALL.to_vec(),
            min_time: Duration::from_millis(10),
            memory: false,
            details: false,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--memory" {
                config.memory = true;
                continue;
            }
            if arg == "--details" {
                config.details = true;
                continue;
            }
            if arg == "--help" {
                println!(
                    "--samples N --cardinalities 64,1024,65536 --widths 24,96 --min-ms 10 --memory --details"
                );
                println!(
                    "--operations {}",
                    Operation::ALL.map(Operation::name).join(",")
                );
                std::process::exit(0);
            }
            let value = args.next().expect("missing option value");
            match arg.as_str() {
                "--samples" => config.samples = value.parse().unwrap(),
                "--cardinalities" => config.counts = numbers(&value),
                "--widths" => config.widths = numbers(&value),
                "--min-ms" => config.min_time = Duration::from_millis(value.parse().unwrap()),
                "--operations" => {
                    config.operations = value
                        .split(',')
                        .map(|name| {
                            Operation::ALL
                                .into_iter()
                                .find(|op| op.name() == name)
                                .unwrap_or_else(|| panic!("unknown operation {name}"))
                        })
                        .collect();
                }
                _ => panic!("unknown option {arg}"),
            }
        }
        assert!(config.samples > 0 && !config.min_time.is_zero());
        assert!(config.counts.iter().all(|&count| count > 0));
        assert!(config.widths.iter().all(|&width| width >= 8));
        config
    }
}

fn numbers(value: &str) -> Vec<usize> {
    value.split(',').map(|part| part.parse().unwrap()).collect()
}

fn keys(count: usize, width: usize, prefix: u8) -> Vec<Bytes> {
    (0..count)
        .map(|index| {
            let mut bytes = vec![prefix; width];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            let key = Bytes::from(bytes);
            // Promotion from owned to shared storage must not occur in timings.
            drop(black_box(key.clone()));
            key
        })
        .collect()
}

fn populate<M: BenchMap>(map: &M, keys: &[Bytes]) {
    for (index, key) in keys.iter().enumerate() {
        map.write(key.clone(), index as u64);
    }
    map.quiesce();
}

fn measure<M: BenchMap>(
    op: Operation,
    keys: &[Bytes],
    misses: &[Bytes],
    seed: &RandomState,
    min_time: Duration,
) -> f64 {
    if op == Operation::ReadWrite {
        let mut elapsed = Duration::ZERO;
        let mut rounds = 0;
        while elapsed < min_time {
            elapsed += concurrent::<M>(keys, seed);
            rounds += 1;
        }
        return elapsed.as_secs_f64() * 1e9 / (rounds * 400_000) as f64;
    }
    let mut elapsed = Duration::ZERO;
    let mut operations = 0usize;
    // Keep a stable working set for repeatable reads and steady-state writes.
    // Rebuilding it between every tiny iteration would make timer accumulation
    // overwhelmingly expensive while measuring only a few microseconds.
    let repetitions = (4096 / keys.len()).clamp(1, 256);
    let mut maps = Vec::with_capacity(repetitions);
    let fresh = matches!(
        op,
        Operation::Delete | Operation::InsertGrowing | Operation::InsertReserved
    );
    let mut reverse_churn = false;
    while elapsed < min_time {
        // Amortize the timer for tiny maps without retaining excessive memory.
        while maps.len() < repetitions {
            let capacity = if op == Operation::InsertGrowing {
                0
            } else {
                keys.len()
            };
            let map = M::empty(capacity, seed.clone());
            if !matches!(op, Operation::InsertGrowing | Operation::InsertReserved) {
                populate(&map, keys);
            }
            maps.push(map);
        }
        let (source, destination) = if op == Operation::Churn && reverse_churn {
            (misses, keys)
        } else {
            (keys, misses)
        };
        let start = Instant::now();
        for map in &maps {
            if op == Operation::Iterate {
                black_box(map.visit());
            } else {
                for (index, key) in source.iter().enumerate() {
                    match op {
                        Operation::Hit => {
                            black_box(map.read(black_box(key)));
                        }
                        Operation::Miss => {
                            black_box(map.read(black_box(&misses[index])));
                        }
                        Operation::Update
                        | Operation::InsertReserved
                        | Operation::InsertGrowing => {
                            map.write(key.clone(), black_box(index as u64));
                        }
                        Operation::Compute => map.compute(key.clone()),
                        Operation::Delete => map.remove(key),
                        Operation::Churn => {
                            map.remove(key);
                            map.write(destination[index].clone(), index as u64);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
        elapsed += start.elapsed();
        // Churn is one remove+insert cycle, iteration is per entry.
        operations += repetitions * keys.len();
        black_box(&maps);
        if fresh {
            maps.clear();
        }
        reverse_churn = !reverse_churn;
    }
    elapsed.as_secs_f64() * 1e9 / operations as f64
}

fn concurrent<M: BenchMap>(keys: &[Bytes], seed: &RandomState) -> Duration {
    let map = M::empty(keys.len(), seed.clone());
    populate(&map, keys);
    let ready = Barrier::new(5);
    let start = Barrier::new(5);
    let count = 100_000usize;
    std::thread::scope(|scope| {
        let mut tasks = Vec::new();
        for worker in 0..4 {
            let map = &map;
            let ready = &ready;
            let start = &start;
            tasks.push(scope.spawn(move || {
                ready.wait();
                start.wait();
                for index in 0..count {
                    let key = &keys[(index * 17 + worker * 251) % keys.len()];
                    if worker == 0 {
                        map.write(key.clone(), index as u64);
                    } else {
                        black_box(map.read(black_box(key)));
                    }
                }
            }));
        }
        ready.wait();
        let timer = Instant::now();
        start.wait();
        for task in tasks {
            task.join().unwrap();
        }
        timer.elapsed()
    })
}

fn measure_scan(keys: &[Bytes], seed: &RandomState, count: usize, min_time: Duration) -> f64 {
    let map = Scannable::empty(keys.len(), seed.clone());
    populate(&map, keys);
    let mut elapsed = Duration::ZERO;
    let mut visited = 0usize;
    while elapsed < min_time {
        let mut cursor = 0;
        let mut found = 0;
        let timer = Instant::now();
        loop {
            cursor = map.pin().scan(cursor, count, |key, value| {
                black_box((key, value));
                found += 1;
            });
            if cursor == 0 {
                break;
            }
        }
        elapsed += timer.elapsed();
        assert_eq!(
            found,
            keys.len(),
            "stable map scan must visit each key once"
        );
        visited += found;
    }
    elapsed.as_secs_f64() * 1e9 / visited as f64
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_unstable_by(f64::total_cmp);
    values[values.len() / 2]
}

fn memory<M: BenchMap>(
    keys: &[Bytes],
    misses: &[Bytes],
    seed: &RandomState,
    mode: &str,
) -> (isize, isize) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let map = M::empty(
        if mode == "reserved" { keys.len() } else { 0 },
        seed.clone(),
    );
    populate(&map, keys);
    if mode == "churn" {
        for key in keys {
            map.remove(key);
        }
        for (index, key) in misses.iter().enumerate() {
            map.write(key.clone(), index as u64);
        }
    }
    if mode == "held-reader" {
        // Model an owned snapshot/scan guard delaying reclamation while the
        // writer replaces every entry. The peak includes all retained versions.
        map.with_reader(|| {
            for (index, key) in keys.iter().enumerate() {
                map.write(key.clone(), index as u64 + 1);
            }
        });
    }
    map.quiesce();
    let inline = size_of::<M>() as isize;
    let live = LIVE.load(Ordering::Relaxed) - baseline + inline;
    let peak = PEAK.load(Ordering::Relaxed) - baseline + inline;
    black_box(&map);
    (live, peak)
}

fn main() {
    let config = Config::parse();
    TRACK.store(config.memory, Ordering::Relaxed);
    if config.memory {
        println!(
            "keys,key_bytes,mode,papaya_live_bytes,map_live_bytes,papaya_peak_bytes,map_peak_bytes,map_over_papaya"
        );
    } else {
        print!("keys,key_bytes,operation,papaya_ns,map_ns,map_over_papaya,samples");
        if config.details {
            print!(",ratio_p10,ratio_p50,ratio_p90");
        }
        println!();
    }
    for &count in &config.counts {
        for &width in &config.widths {
            let keys = keys(count, width, b'k');
            let misses = keys_for_misses(count, width);
            if config.memory {
                for mode in ["reserved", "grown", "churn", "held-reader"] {
                    let seed = RandomState::new();
                    // Warm lazy thread/collector metadata before measuring.
                    drop(Original::empty(0, seed.clone()));
                    drop(Scannable::empty(0, seed.clone()));
                    let a = memory::<Original>(&keys, &misses, &seed, mode);
                    let b = memory::<Scannable>(&keys, &misses, &seed, mode);
                    println!(
                        "{count},{width},{mode},{},{},{},{},{:.3}",
                        a.0,
                        b.0,
                        a.1,
                        b.1,
                        b.0 as f64 / a.0 as f64
                    );
                }
                continue;
            }
            for &op in &config.operations {
                let mut original = Vec::with_capacity(config.samples);
                let mut scannable = Vec::with_capacity(config.samples);
                for sample in 0..=config.samples {
                    let seed = RandomState::new();
                    if matches!(op, Operation::Scan10 | Operation::Scan100) {
                        let hint = if op == Operation::Scan10 { 10 } else { 100 };
                        let result = measure_scan(&keys, &seed, hint, config.min_time);
                        if sample > 0 {
                            scannable.push(result);
                        }
                        continue;
                    }
                    let run_original =
                        || measure::<Original>(op, &keys, &misses, &seed, config.min_time);
                    let run_scannable =
                        || measure::<Scannable>(op, &keys, &misses, &seed, config.min_time);
                    let (a, b) = if sample % 2 == 0 {
                        (run_original(), run_scannable())
                    } else {
                        let b = run_scannable();
                        (run_original(), b)
                    };
                    if sample > 0 {
                        original.push(a);
                        scannable.push(b);
                    }
                }
                let mut ratios: Vec<_> = original
                    .iter()
                    .zip(&scannable)
                    .map(|(a, b)| b / a)
                    .collect();
                ratios.sort_unstable_by(f64::total_cmp);
                let b = median(&mut scannable);
                if original.is_empty() {
                    print!("{count},{width},{},,{b:.2},,{}", op.name(), config.samples);
                } else {
                    let a = median(&mut original);
                    print!(
                        "{count},{width},{},{a:.2},{b:.2},{:.3},{}",
                        op.name(),
                        b / a,
                        config.samples
                    );
                }
                if config.details {
                    if ratios.is_empty() {
                        print!(",,,");
                    } else {
                        print!(
                            ",{:.3},{:.3},{:.3}",
                            ratios[(ratios.len() - 1) / 10],
                            ratios[ratios.len() / 2],
                            ratios[(ratios.len() - 1) * 9 / 10]
                        );
                    }
                }
                println!();
            }
        }
    }
}

fn keys_for_misses(count: usize, width: usize) -> Vec<Bytes> {
    // The first 8 bytes store the index; use a disjoint numeric domain even for
    // --widths 8, where no prefix bytes remain to distinguish misses.
    (0..count)
        .map(|index| {
            let mut bytes = vec![b'm'; width];
            bytes[..8].copy_from_slice(&((index as u64) | (1 << 63)).to_le_bytes());
            let key = Bytes::from(bytes);
            drop(black_box(key.clone()));
            key
        })
        .collect()
}
