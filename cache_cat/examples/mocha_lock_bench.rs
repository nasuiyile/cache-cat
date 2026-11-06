//! Concurrent resize and reclamation workloads for Mocha's map.
//!
//! Run: cargo run -p cache_cat --release --example mocha_lock_bench --
//! --samples 9 --keys 65536 --width 24
//! Options: --workload single-growth,single-churn,growth-read,growth-expiry,growth-hot
//! --memory --progress --expiry-rounds 4 (fixed expiration attempts per writer cycle)
//!
//! Default compares upstream papaya with the current map. The task's isolated
//! probe can additionally compile with `--cfg mocha_lock_baseline` after saving
//! the prior map at target/mocha-lock-baseline/map. That optional comparison is
//! deliberately absent from normal builds and does not require a library API.
//!
//! Each sample shares the same RandomState across variants and rotates run
//! order. One warmup sample is discarded. Threads and key buffers are prepared
//! before timing. One logical writer inserts --keys new keys while three readers
//! run, or two readers plus an auxiliary expiration worker. growth-hot also
//! updates one hot key each cycle while the expiration worker removes its current
//! version until the writer finishes. This exercises concurrent resize help and
//! same-key contention throughout growth; it
//! does not artificially suspend a copier inside its critical section.
//! --expiry-rounds fixes total auxiliary work to rounds * keys instead of
//! stopping it when the writer finishes; this supports controlled followups.
//! single-growth and single-churn run only the writer; the latter prepopulates
//! the table and measures removal/reinsertion cycles without concurrent actors.
//!
//! Latency is per complete writer cycle (insert, plus hot-key update when used),
//! measured with Instant around each cycle. Reported percentiles are medians of
//! per-sample percentiles, not pooled percentiles. Throughput includes timer
//! overhead equally; readers check completion once per 128 operations. Memory
//! mode disables latency recording and counts allocator requests during the
//! measured concurrent work, after thread/collector initialization. Peak-extra
//! bytes are incremental live allocation above that barrier, not RSS. Allocator
//! counters are disabled in timing mode, whose allocation columns remain zero.
//! Paired elapsed ratios compare the same sample/hash seed with the saved
//! baseline (or papaya if absent); their quantiles describe run-to-run noise.

#![allow(unexpected_cfgs)]

#[allow(dead_code, unused_imports)]
#[path = "../src/mocha/map/mod.rs"]
mod map;

#[cfg(mocha_lock_baseline)]
#[allow(dead_code, unused_imports)]
#[path = "../../target/mocha-lock-baseline/map/mod.rs"]
mod before_map;

use bytes::Bytes;
use seize::Guard;
use std::alloc::{GlobalAlloc, Layout};
use std::collections::hash_map::RandomState;
use std::hint::black_box;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::time::Instant;

struct Allocator;
static TRACK: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

fn allocated(bytes: usize) {
    if TRACK.load(Ordering::Relaxed) {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(bytes, Ordering::Relaxed);
        PEAK.fetch_max(
            LIVE.fetch_add(bytes as isize, Ordering::Relaxed) + bytes as isize,
            Ordering::Relaxed,
        );
    }
}
fn released(bytes: usize) {
    if TRACK.load(Ordering::Relaxed) {
        LIVE.fetch_sub(bytes as isize, Ordering::Relaxed);
    }
}

// SAFETY: Requests and pointers pass unchanged to one MiMalloc allocator.
// Counter updates never access the allocation and never allocate themselves.
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
        released(layout.size());
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, size) };
        if !next.is_null() {
            released(layout.size());
            allocated(size);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

type Payload = [u64; 7];
type Original = papaya::HashMap<Bytes, Payload>;
type Current = map::HashMap<Bytes, Payload>;
#[cfg(mocha_lock_baseline)]
type Before = before_map::HashMap<Bytes, Payload>;

trait BenchMap: Sync {
    fn new(seed: RandomState) -> Self;
    fn read(&self, key: &Bytes) -> Option<u64>;
    fn write(&self, key: Bytes, version: u64);
    fn expire(&self, key: Bytes, expected: Option<u64>);
    fn flush(&self);
}

macro_rules! bench_map {
    ($ty:ty, $operation:path) => {
        impl BenchMap for $ty {
            fn new(seed: RandomState) -> Self {
                Self::with_hasher(seed)
            }
            #[inline]
            fn read(&self, key: &Bytes) -> Option<u64> {
                self.pin().get(key).map(|v| v[0])
            }
            #[inline]
            fn write(&self, key: Bytes, version: u64) {
                black_box(self.pin().insert(key, [version; 7]));
            }
            #[inline]
            fn expire(&self, key: Bytes, expected: Option<u64>) {
                use $operation as Operation;
                black_box(self.pin().compute(key, |entry| {
                    if entry.is_some_and(|(_, value)| Some(value[0]) == expected) {
                        Operation::<Payload, ()>::Remove
                    } else {
                        Operation::Abort(())
                    }
                }));
            }
            fn flush(&self) {
                self.guard().flush();
            }
        }
    };
}
bench_map!(Original, papaya::Operation);
bench_map!(Current, map::Operation);
#[cfg(mocha_lock_baseline)]
bench_map!(Before, before_map::Operation);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    SingleGrowth,
    SingleChurn,
    Read,
    Expiry,
    Hot,
}
impl Workload {
    const ALL: [Self; 5] = [
        Self::SingleGrowth,
        Self::SingleChurn,
        Self::Read,
        Self::Expiry,
        Self::Hot,
    ];
    fn name(self) -> &'static str {
        match self {
            Self::SingleGrowth => "single-growth",
            Self::SingleChurn => "single-churn",
            Self::Read => "growth-read",
            Self::Expiry => "growth-expiry",
            Self::Hot => "growth-hot",
        }
    }
}

struct Config {
    samples: usize,
    keys: usize,
    width: usize,
    workloads: Vec<Workload>,
    memory: bool,
    expiry_rounds: Option<usize>,
    progress: bool,
}
impl Config {
    fn parse() -> Self {
        let mut config = Self {
            samples: 9,
            keys: 65_536,
            width: 24,
            workloads: Workload::ALL.to_vec(),
            memory: false,
            expiry_rounds: None,
            progress: false,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--memory" {
                config.memory = true;
                continue;
            }
            if arg == "--progress" {
                config.progress = true;
                continue;
            }
            if arg == "--help" {
                println!(
                    "--samples 9 --keys 65536 --width 24 --workload single-growth,single-churn,growth-read,growth-expiry,growth-hot --memory --progress --expiry-rounds 4"
                );
                std::process::exit(0);
            }
            let value = args.next().expect("missing option value");
            match arg.as_str() {
                "--samples" => config.samples = value.parse().unwrap(),
                "--keys" => config.keys = value.parse().unwrap(),
                "--width" => config.width = value.parse().unwrap(),
                "--expiry-rounds" => config.expiry_rounds = Some(value.parse().unwrap()),
                "--workload" => {
                    config.workloads = value
                        .split(',')
                        .map(|name| {
                            Workload::ALL
                                .into_iter()
                                .find(|w| w.name() == name)
                                .unwrap_or_else(|| panic!("unknown workload {name}"))
                        })
                        .collect()
                }
                _ => panic!("unknown option {arg}"),
            }
        }
        assert!(config.samples > 0 && config.keys >= 256 && config.width >= 8);
        assert!(
            config
                .expiry_rounds
                .is_none_or(|rounds| rounds > 0 && rounds.checked_mul(config.keys).is_some())
        );
        config
    }
}

fn keys(count: usize, domain: u64, width: usize) -> Vec<Bytes> {
    (0..count)
        .map(|index| {
            let mut bytes = vec![b'k'; width];
            bytes[..8].copy_from_slice(&(domain | index as u64).to_le_bytes());
            let key = Bytes::from(bytes);
            drop(black_box(key.clone()));
            key
        })
        .collect()
}

#[derive(Default)]
struct ResultRow {
    seconds: f64,
    cycles: f64,
    reads: f64,
    expiry: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    allocation_calls: f64,
    allocation_bytes: f64,
    peak_extra_bytes: f64,
}

// A failed writer must release the other actors before scoped joins propagate
// its panic. Otherwise a correctness failure could look like a map deadlock.
struct ActorCompletion<'a> {
    remaining: &'a AtomicUsize,
    writer_finished: Option<&'a AtomicBool>,
}
impl Drop for ActorCompletion<'_> {
    fn drop(&mut self) {
        if let Some(finished) = self.writer_finished {
            finished.store(true, Ordering::Release);
        }
        self.remaining.fetch_sub(1, Ordering::Release);
    }
}

fn run<M: BenchMap>(
    config: &Config,
    workload: Workload,
    seed: RandomState,
    anchors: &[Bytes],
    additions: &[Bytes],
    expiring: &[Bytes],
    hot: &Bytes,
) -> ResultRow {
    let map = M::new(seed);
    for (index, key) in anchors.iter().enumerate() {
        map.write(key.clone(), index as u64);
    }
    if workload == Workload::Expiry {
        for (index, key) in expiring.iter().enumerate() {
            map.write(key.clone(), index as u64);
        }
    }
    if workload == Workload::Hot {
        map.write(hot.clone(), 0);
    }
    if workload == Workload::SingleChurn {
        for (index, key) in additions.iter().enumerate() {
            map.write(key.clone(), index as u64);
        }
    }
    map.flush();
    let has_expirer = matches!(workload, Workload::Expiry | Workload::Hot);
    let reader_count = if matches!(workload, Workload::SingleGrowth | Workload::SingleChurn) {
        0
    } else if has_expirer {
        2
    } else {
        3
    };
    let actors = 1 + usize::from(has_expirer) + reader_count;
    let ready = Barrier::new(actors + 1);
    let start = Barrier::new(actors + 1);
    let remaining = AtomicUsize::new(if has_expirer { 2 } else { 1 });
    let writer_finished = AtomicBool::new(false);
    let record = !config.memory;
    let (
        elapsed,
        reads,
        expiry,
        mut latencies,
        allocation_calls,
        allocation_bytes,
        peak_extra_bytes,
    ) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut latencies = Vec::with_capacity(if record { additions.len() } else { 0 });
            black_box(map.read(&anchors[0]));
            ready.wait();
            start.wait();
            let _completion = ActorCompletion {
                remaining: &remaining,
                writer_finished: Some(&writer_finished),
            };
            for (index, key) in additions.iter().enumerate() {
                let timer = record.then(Instant::now);
                if workload == Workload::SingleChurn {
                    map.expire(key.clone(), Some(index as u64));
                }
                map.write(key.clone(), index as u64);
                if workload == Workload::Hot {
                    map.write(hot.clone(), index as u64 + 1);
                }
                if let Some(timer) = timer {
                    latencies.push(timer.elapsed().as_nanos() as u64);
                }
            }
            latencies
        });
        let expirer = has_expirer.then(|| {
            scope.spawn(|| {
                black_box(map.read(&anchors[0]));
                ready.wait();
                start.wait();
                let _completion = ActorCompletion {
                    remaining: &remaining,
                    writer_finished: None,
                };
                let count = if workload == Workload::Expiry {
                    for (index, key) in expiring.iter().enumerate() {
                        map.expire(key.clone(), Some(index as u64));
                    }
                    expiring.len()
                } else if let Some(rounds) = config.expiry_rounds {
                    let count = additions.len() * rounds;
                    for _ in 0..count {
                        map.expire(hot.clone(), map.read(hot));
                    }
                    count
                } else {
                    let mut count = 0;
                    while !writer_finished.load(Ordering::Acquire) {
                        for _ in 0..64 {
                            map.expire(hot.clone(), map.read(hot));
                        }
                        count += 64;
                    }
                    count
                };
                count
            })
        });
        let mut readers = Vec::new();
        for reader in 0..reader_count {
            let map = &map;
            let ready = &ready;
            let start = &start;
            let remaining = &remaining;
            readers.push(scope.spawn(move || {
                black_box(map.read(&anchors[0]));
                ready.wait();
                start.wait();
                let mut reads = 0usize;
                while remaining.load(Ordering::Acquire) != 0 {
                    for index in 0..128 {
                        // Anchors are never removed: this also detects a lost
                        // stable key during concurrent growth/reclamation.
                        assert!(
                            black_box(
                                map.read(&anchors[(reads + index * 17 + reader) % anchors.len()])
                            )
                            .is_some()
                        );
                    }
                    reads += 128;
                }
                reads
            }));
        }
        ready.wait();
        let allocation_start = ALLOC_CALLS.load(Ordering::Relaxed);
        let byte_start = ALLOC_BYTES.load(Ordering::Relaxed);
        let live_start = LIVE.load(Ordering::Relaxed);
        PEAK.store(live_start, Ordering::Relaxed);
        let timer = Instant::now();
        start.wait();
        let latencies = writer.join().unwrap();
        let expiry = expirer.map_or(0, |task| task.join().unwrap());
        let reads = readers
            .into_iter()
            .map(|task| task.join().unwrap())
            .sum::<usize>();
        let elapsed = timer.elapsed();
        (
            elapsed,
            reads,
            expiry,
            latencies,
            ALLOC_CALLS.load(Ordering::Relaxed) - allocation_start,
            ALLOC_BYTES.load(Ordering::Relaxed) - byte_start,
            PEAK.load(Ordering::Relaxed) - live_start,
        )
    });
    latencies.sort_unstable();
    let percentile = |percent: usize| {
        latencies
            .get(latencies.len().saturating_sub(1) * percent / 100)
            .copied()
            .unwrap_or(0) as f64
    };
    ResultRow {
        seconds: elapsed.as_secs_f64(),
        cycles: additions.len() as f64,
        reads: reads as f64,
        expiry: expiry as f64,
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        max: percentile(100),
        allocation_calls: allocation_calls as f64,
        allocation_bytes: allocation_bytes as f64,
        peak_extra_bytes: peak_extra_bytes as f64,
    }
}

fn median(rows: &[ResultRow], value: impl Fn(&ResultRow) -> f64) -> f64 {
    let mut numbers: Vec<_> = rows.iter().map(value).collect();
    numbers.sort_unstable_by(f64::total_cmp);
    numbers[numbers.len() / 2]
}

fn main() {
    let config = Config::parse();
    TRACK.store(config.memory, Ordering::Relaxed);
    let anchors = keys(64, 0, config.width);
    let additions = keys(config.keys, 1 << 62, config.width);
    let expiring = keys(config.keys / 4, 1 << 63, config.width);
    let hot = Bytes::from_static(b"hot-key-during-resize");
    #[cfg(mocha_lock_baseline)]
    let names = ["papaya", "before", "current"];
    #[cfg(not(mocha_lock_baseline))]
    let names = ["papaya", "current"];
    println!(
        "keys,key_bytes,workload,map,samples,writer_cycles_per_s,reader_ops_per_s,expiry_ops_per_s,write_p50_ns,write_p95_ns,write_p99_ns,write_max_ns,alloc_calls,alloc_bytes,peak_extra_bytes,reference,elapsed_ratio_p10,elapsed_ratio_p50,elapsed_ratio_p90"
    );
    for &workload in &config.workloads {
        let mut results: Vec<Vec<ResultRow>> = (0..names.len()).map(|_| Vec::new()).collect();
        for sample in 0..=config.samples {
            let seed = RandomState::new();
            for step in 0..names.len() {
                let variant = (step + sample) % names.len();
                if config.progress {
                    eprintln!(
                        "start sample={sample}/{} workload={} map={}",
                        config.samples,
                        workload.name(),
                        names[variant]
                    );
                }
                let row = match names[variant] {
                    "papaya" => run::<Original>(
                        &config,
                        workload,
                        seed.clone(),
                        &anchors,
                        &additions,
                        &expiring,
                        &hot,
                    ),
                    #[cfg(mocha_lock_baseline)]
                    "before" => run::<Before>(
                        &config,
                        workload,
                        seed.clone(),
                        &anchors,
                        &additions,
                        &expiring,
                        &hot,
                    ),
                    "current" => run::<Current>(
                        &config,
                        workload,
                        seed.clone(),
                        &anchors,
                        &additions,
                        &expiring,
                        &hot,
                    ),
                    _ => unreachable!(),
                };
                if config.progress {
                    eprintln!(
                        "done sample={sample}/{} workload={} map={} measured_seconds={:.3}",
                        config.samples,
                        workload.name(),
                        names[variant],
                        row.seconds
                    );
                }
                if sample > 0 {
                    results[variant].push(row);
                }
            }
        }
        let reference = names.iter().position(|name| *name == "before").unwrap_or(0);
        for (index, name) in names.into_iter().enumerate() {
            let rows = &results[index];
            let mut ratios: Vec<_> = rows
                .iter()
                .zip(&results[reference])
                .map(|(row, baseline)| row.seconds / baseline.seconds)
                .collect();
            ratios.sort_unstable_by(f64::total_cmp);
            println!(
                "{},{},{},{},{},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{},{:.3},{:.3},{:.3}",
                config.keys,
                config.width,
                workload.name(),
                name,
                config.samples,
                median(&rows, |r| r.cycles / r.seconds),
                median(&rows, |r| r.reads / r.seconds),
                median(&rows, |r| r.expiry / r.seconds),
                median(&rows, |r| r.p50),
                median(&rows, |r| r.p95),
                median(&rows, |r| r.p99),
                median(&rows, |r| r.max),
                median(&rows, |r| r.allocation_calls),
                median(&rows, |r| r.allocation_bytes),
                median(&rows, |r| r.peak_extra_bytes),
                names[reference],
                ratios[(ratios.len() - 1) / 10],
                ratios[ratios.len() / 2],
                ratios[(ratios.len() - 1) * 9 / 10]
            );
        }
    }
}
