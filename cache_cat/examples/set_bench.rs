//! Compare Redis Set storage with the former standard HashSet.
//!
//! Run with `cargo run -p cache_cat --release --example set_bench -- 3 65536`.
//! Arguments: sample count (default 3), largest cardinality (default 65536).
//! Optional flags: --operations clone-alone,clone-grown,snapshot-add-grown
//! --cardinalities 8191,8192,8193,10000 --widths 12,96 --min-ms 15
//! --batch-members 262144 --max-batch-mib 32 --min-batches 3 --warmup-pairs 2.
//! Explicit cardinalities override the largest-cardinality argument. `memory`
//! skips timings. `--help` prints the full operation list and timing details.
//!
//! Each sample accumulates at least --min-ms (default 3) of measured work for
//! BOTH containers and at least --min-batches paired batches. ABBA order and
//! discarded warmup pairs reduce order/allocator bias; seeds stay fixed within
//! a sample and differ between samples. This measures a warmed allocator and
//! warmed sources, not cold-page or end-to-end latency.
//!
//! Bytes are promoted to shared storage before any timing, including filtered
//! insertion runs. Setup and container destruction are excluded. Removed
//! members' reference decrements are part of the measured mutation. Snapshots
//! retain the old source alongside cloned outputs and verify isolation outside
//! the timer. Batch sizes have a conservative memory bound, except that at least
//! one source and result must fit. Default runs are intended to take tens of
//! seconds; focused runs can increase samples and minimum time for diagnosis.
//!
//! Hashers are production defaults: std SipHash versus ScanSet aHash. Mutexes,
//! RESP and Raft are excluded. The CSV unit column distinguishes member costs,
//! complete snapshot writes and hot command cycles; runtime/batch columns make
//! one-off, short measurements visible.

use bytes::Bytes;
use cache_cat::raft::types::core::structure::scan_set::ScanSet;
use std::collections::{HashSet, hash_map::RandomState};
use std::hint::black_box;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type StandardSet = HashSet<Bytes>;
type ScannableSet = ScanSet<Bytes>;

const CARDINALITIES: [usize; 8] = [1, 4, 8, 64, 1024, 10_000, 65_536, 262_144];
const MAX_BATCH_REPETITIONS: usize = 4096;

#[derive(Clone)]
struct Hashers {
    standard: RandomState,
    scannable: ahash::RandomState,
}

#[derive(Clone)]
struct Seeds {
    source: Hashers,
    other: Hashers,
}

impl Seeds {
    fn new() -> Self {
        let new_hashers = || Hashers {
            standard: RandomState::new(),
            scannable: ahash::RandomState::new(),
        };
        Self {
            source: new_hashers(),
            other: new_hashers(),
        }
    }
}

trait Set: Clone {
    fn empty(capacity: usize, state: Hashers) -> Self;
    fn len(&self) -> usize;
    fn contains(&self, member: &Bytes) -> bool;
    fn insert(&mut self, member: Bytes) -> bool;
    fn remove(&mut self, member: &Bytes) -> bool;
    fn retain_half(&mut self);
    fn retain_intersection(&mut self, other: &Self);
    fn retain_difference(&mut self, other: &Self);
    fn visit(&self) -> usize;
}

macro_rules! impl_set {
    ($set:ty, $hasher:ident) => {
        impl Set for $set {
            fn empty(capacity: usize, state: Hashers) -> Self {
                Self::with_capacity_and_hasher(capacity, state.$hasher)
            }
            fn len(&self) -> usize {
                Self::len(self)
            }
            fn contains(&self, member: &Bytes) -> bool {
                Self::contains(self, member)
            }
            fn insert(&mut self, member: Bytes) -> bool {
                Self::insert(self, member)
            }
            fn remove(&mut self, member: &Bytes) -> bool {
                Self::remove(self, member)
            }
            fn retain_half(&mut self) {
                self.retain(|member| black_box(member[member.len() - 8]) & 1 == 0);
            }
            fn retain_intersection(&mut self, other: &Self) {
                self.retain(|member| other.contains(black_box(member)));
            }
            fn retain_difference(&mut self, other: &Self) {
                self.retain(|member| !other.contains(black_box(member)));
            }
            fn visit(&self) -> usize {
                self.iter().fold(0usize, |sum, member| {
                    sum.wrapping_add(black_box(member).len())
                })
            }
        }
    };
}
impl_set!(StandardSet, standard);
impl_set!(ScannableSet, scannable);

#[derive(Clone, Copy)]
enum Operation {
    ContainsHit,
    ContainsMiss,
    InsertDuplicate,
    InsertReserved,
    InsertGrowing,
    RemoveChurn,
    IterateDense,
    IterateSparse,
    RetainHalf,
    RetainSparse,
    Intersection,
    Difference,
    CloneAlone,
    CloneGrown,
    CloneSparse,
    SnapshotAddReserved,
    SnapshotAddGrown,
    SnapshotRemoveGrown,
    HotSetCycle,
}

impl Operation {
    const ALL: [Self; 19] = [
        Self::ContainsHit,
        Self::ContainsMiss,
        Self::InsertDuplicate,
        Self::InsertReserved,
        Self::InsertGrowing,
        Self::RemoveChurn,
        Self::IterateDense,
        Self::IterateSparse,
        Self::RetainHalf,
        Self::RetainSparse,
        Self::Intersection,
        Self::Difference,
        Self::CloneAlone,
        Self::CloneGrown,
        Self::CloneSparse,
        Self::SnapshotAddReserved,
        Self::SnapshotAddGrown,
        Self::SnapshotRemoveGrown,
        Self::HotSetCycle,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::ContainsHit => "contains-hit",
            Self::ContainsMiss => "contains-miss",
            Self::InsertDuplicate => "insert-duplicate",
            Self::InsertReserved => "insert-reserved",
            Self::InsertGrowing => "insert-growing",
            Self::RemoveChurn => "remove-churn",
            Self::IterateDense => "iterate-dense",
            Self::IterateSparse => "iterate-sparse",
            Self::RetainHalf => "retain-half",
            Self::RetainSparse => "retain-sparse",
            Self::Intersection => "intersection",
            Self::Difference => "difference",
            Self::CloneAlone => "clone-alone",
            Self::CloneGrown => "clone-grown",
            Self::CloneSparse => "clone-sparse",
            Self::SnapshotAddReserved => "snapshot-add-reserved",
            Self::SnapshotAddGrown => "snapshot-add-grown",
            Self::SnapshotRemoveGrown => "snapshot-remove-grown",
            Self::HotSetCycle => "hot-set-cycle",
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Self::SnapshotAddReserved | Self::SnapshotAddGrown | Self::SnapshotRemoveGrown => {
                "ns/write"
            }
            Self::HotSetCycle => "ns/cycle",
            _ => "ns/member",
        }
    }

    fn units_per_repetition(self, count: usize) -> usize {
        match self {
            Self::IterateSparse | Self::RetainSparse | Self::CloneSparse => (count / 16).max(1),
            Self::SnapshotAddReserved | Self::SnapshotAddGrown | Self::SnapshotRemoveGrown => 1,
            _ => count,
        }
    }
}

fn fill<S: Set>(members: &[Bytes], state: Hashers) -> S {
    fill_with_capacity(members, members.len(), state)
}

fn fill_with_capacity<S: Set>(members: &[Bytes], capacity: usize, state: Hashers) -> S {
    let mut set = S::empty(capacity, state);
    // Custom cardinalities need not be coprime to 53. Select a coprime stride
    // so every member is inserted exactly once, in a different order to reads.
    let mut stride = 53usize;
    loop {
        let (mut left, mut right) = (stride, members.len());
        while right != 0 {
            (left, right) = (right, left % right);
        }
        if left == 1 {
            break;
        }
        stride += 2;
    }
    for offset in 0..members.len() {
        let index = offset * stride % members.len();
        set.insert(members[index].clone());
    }
    assert_eq!(set.len(), members.len());
    set
}

fn make_sparse<S: Set>(set: &mut S, members: &[Bytes]) -> usize {
    // Do not shrink the allocation: this models repeated SREM/SPOP. Members
    // are shuffled, so the retained subset is not biased toward even IDs.
    let remaining = (members.len() / 16).max(1);
    for member in &members[remaining..] {
        set.remove(member);
    }
    remaining
}

struct Prepared<S> {
    source: S,
    other: Option<S>,
    sparse_pool: Vec<S>,
    state: Hashers,
}

impl<S: Set> Prepared<S> {
    fn new(operation: Operation, members: &[Bytes], misses: &[Bytes], seeds: &Seeds) -> Self {
        let grown = matches!(
            operation,
            Operation::CloneGrown
                | Operation::CloneSparse
                | Operation::SnapshotAddGrown
                | Operation::SnapshotRemoveGrown
        );
        let capacity = if grown { 0 } else { members.len() };
        let mut source = if matches!(
            operation,
            Operation::InsertReserved | Operation::InsertGrowing
        ) {
            S::empty(0, seeds.source.clone())
        } else {
            fill_with_capacity::<S>(members, capacity, seeds.source.clone())
        };
        if matches!(
            operation,
            Operation::IterateSparse | Operation::RetainSparse | Operation::CloneSparse
        ) {
            make_sparse(&mut source, members);
        }
        let other = if matches!(operation, Operation::Intersection | Operation::Difference) {
            let overlap = members.len() / 2;
            let other_members: Vec<_> = members[..overlap]
                .iter()
                .chain(&misses[overlap..])
                .cloned()
                .collect();
            // Equal cardinality, half overlapping, with an independent seed.
            Some(fill::<S>(&other_members, seeds.other.clone()))
        } else {
            None
        };
        Self {
            source,
            other,
            sparse_pool: Vec::new(),
            state: seeds.source.clone(),
        }
    }
}

fn measure_batch<S: Set>(
    operation: Operation,
    members: &[Bytes],
    misses: &[Bytes],
    prepared: &mut Prepared<S>,
    loops: usize,
) -> Duration {
    let count = members.len();
    match operation {
        Operation::InsertReserved | Operation::InsertGrowing => {
            let capacity = if matches!(operation, Operation::InsertReserved) {
                count
            } else {
                0
            };
            let mut sets: Vec<_> = (0..loops)
                .map(|_| S::empty(capacity, prepared.state.clone()))
                .collect();
            let start = Instant::now();
            for set in &mut sets {
                for member in members {
                    black_box(set.insert(black_box(member).clone()));
                }
            }
            let elapsed = start.elapsed();
            black_box(&sets);
            elapsed
        }
        Operation::RetainHalf => {
            let mut sets: Vec<_> = (0..loops).map(|_| prepared.source.clone()).collect();
            let start = Instant::now();
            for set in &mut sets {
                black_box(set).retain_half();
            }
            let elapsed = start.elapsed();
            black_box(&sets);
            elapsed
        }
        Operation::RetainSparse => {
            if prepared.sparse_pool.len() != loops {
                // Build each sparse table once. Clone may intentionally compact
                // sparse buckets, which would turn this into a dense benchmark.
                prepared.sparse_pool = (0..loops)
                    .map(|_| {
                        let mut set = fill::<S>(members, prepared.state.clone());
                        make_sparse(&mut set, members);
                        set
                    })
                    .collect();
            }
            let remaining = (count / 16).max(1);
            for set in &mut prepared.sparse_pool {
                // Restore the same live subset without cloning, shrinking, or
                // rebuilding the full table. This work stays outside the timer.
                for member in &members[..remaining] {
                    set.insert(member.clone());
                }
                assert_eq!(set.len(), remaining);
            }
            let start = Instant::now();
            for set in &mut prepared.sparse_pool {
                black_box(set).retain_half();
            }
            let elapsed = start.elapsed();
            black_box(&prepared.sparse_pool);
            elapsed
        }
        Operation::Intersection | Operation::Difference => {
            let mut sets: Vec<_> = (0..loops).map(|_| prepared.source.clone()).collect();
            let other = prepared.other.as_ref().unwrap();
            let start = Instant::now();
            match operation {
                Operation::Intersection => {
                    for set in &mut sets {
                        black_box(set).retain_intersection(black_box(other));
                    }
                }
                Operation::Difference => {
                    for set in &mut sets {
                        black_box(set).retain_difference(black_box(other));
                    }
                }
                _ => unreachable!(),
            }
            let elapsed = start.elapsed();
            black_box(&sets);
            elapsed
        }
        Operation::CloneAlone
        | Operation::CloneGrown
        | Operation::CloneSparse
        | Operation::SnapshotAddReserved
        | Operation::SnapshotAddGrown
        | Operation::SnapshotRemoveGrown => {
            let source = &prepared.source;
            let source_len = source.len();
            let mut clones = Vec::with_capacity(loops);
            let start = Instant::now();
            for _ in 0..loops {
                let mut cloned = black_box(source).clone();
                match operation {
                    Operation::SnapshotAddReserved | Operation::SnapshotAddGrown => {
                        black_box(cloned.insert(black_box(&misses[0]).clone()));
                    }
                    Operation::SnapshotRemoveGrown => {
                        black_box(cloned.remove(black_box(&members[0])));
                    }
                    _ => {}
                }
                // Preserve both versions until the timing ends, matching a
                // concurrent full snapshot holding the old value during writes.
                clones.push(cloned);
            }
            let elapsed = start.elapsed();
            assert_eq!(source.len(), source_len);
            assert!(source.contains(&members[0]));
            assert!(!source.contains(&misses[0]));
            for cloned in &clones {
                match operation {
                    Operation::SnapshotAddReserved | Operation::SnapshotAddGrown => {
                        assert_eq!(cloned.len(), source_len + 1);
                        assert!(cloned.contains(&misses[0]));
                    }
                    Operation::SnapshotRemoveGrown => {
                        assert_eq!(cloned.len(), source_len - 1);
                        assert!(!cloned.contains(&members[0]));
                    }
                    _ => assert_eq!(cloned.len(), source_len),
                }
            }
            black_box(&clones);
            elapsed
        }
        _ => {
            let set = &mut prepared.source;
            let start = Instant::now();
            for _ in 0..loops {
                match operation {
                    Operation::ContainsHit => {
                        for member in members {
                            black_box(set.contains(black_box(member)));
                        }
                    }
                    Operation::ContainsMiss => {
                        for member in misses {
                            black_box(set.contains(black_box(member)));
                        }
                    }
                    Operation::InsertDuplicate => {
                        for member in members {
                            black_box(set.insert(black_box(member).clone()));
                        }
                    }
                    Operation::RemoveChurn => {
                        for member in members {
                            black_box(set.remove(black_box(member)));
                            black_box(set.insert(member.clone()));
                        }
                    }
                    Operation::IterateDense | Operation::IterateSparse => {
                        black_box(black_box(&*set).visit());
                    }
                    Operation::HotSetCycle => {
                        // A hot Set: three hits, one miss, a duplicate SADD,
                        // followed by a remove/reinsert pair. The cardinality
                        // stays fixed; --cardinalities focuses small hot sets.
                        for (member, miss) in members.iter().zip(misses) {
                            for _ in 0..3 {
                                black_box(set.contains(black_box(member)));
                            }
                            black_box(set.contains(black_box(miss)));
                            black_box(set.insert(member.clone()));
                            black_box(set.remove(member));
                            black_box(set.insert(member.clone()));
                        }
                    }
                    _ => unreachable!(),
                }
            }
            let elapsed = start.elapsed();
            black_box(set);
            elapsed
        }
    }
}

fn make_members(count: usize, width: usize, prefix: u8) -> Vec<Bytes> {
    let mut result: Vec<_> = (0..count)
        .map(|index| {
            let mut member = vec![prefix; width];
            member[width - 8..].copy_from_slice(&(index as u64).to_le_bytes());
            // Bytes::from(Vec) can defer its shared control-block allocation
            // until the first clone. Promote before ALL timed workloads, even
            // when --operations selects insertion as the first/only row.
            let member = Bytes::from(member);
            let shared = member.clone();
            drop(member);
            shared
        })
        .collect();
    let mut random = 0x37ab_9845_168c_27e1u64;
    for index in (1..count).rev() {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        result.swap(index, random as usize % (index + 1));
    }
    result
}

fn median(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn print_memory(cardinalities: &[usize]) {
    println!("# Table allocations exclude member payloads and allocator overhead.");
    println!(
        "members,std_table_estimated_bytes,scan_reserved_table_bytes,scan_grown_table_bytes,std_inline_bytes,scan_inline_bytes"
    );
    for &count in cardinalities {
        let standard = StandardSet::with_capacity(count);
        let scannable = ScannableSet::with_capacity(count);
        let mut grown = ScannableSet::new();
        for index in 0..count {
            grown.insert(Bytes::copy_from_slice(&index.to_le_bytes()));
        }
        // Approximate SwissTable's allocation from its reported capacity.
        // Standard HashSet exposes no actual allocation size. This estimate is
        // for fresh tables; tombstones can make capacity understate allocation.
        let capacity = standard.capacity();
        let buckets = if capacity < 4 {
            4
        } else if capacity < 8 {
            8
        } else {
            ((capacity * 8 + 6) / 7).next_power_of_two()
        };
        let data = buckets * size_of::<Bytes>();
        let estimated = data + (16 - data % 16) % 16 + buckets + 16;
        println!(
            "{count},{estimated},{},{},{},{}",
            scannable.allocated_bytes(),
            grown.allocated_bytes(),
            size_of::<StandardSet>(),
            size_of::<ScannableSet>()
        );
    }
}

struct Config {
    samples: usize,
    cardinalities: Vec<usize>,
    widths: Vec<usize>,
    operations: Vec<Operation>,
    min_time: Duration,
    min_batches: usize,
    warmup_pairs: usize,
    batch_members: usize,
    max_batch_bytes: usize,
    memory_only: bool,
}

impl Config {
    fn parse() -> Self {
        let mut config = Self {
            samples: 3,
            cardinalities: Vec::new(),
            widths: vec![12, 96],
            operations: Operation::ALL.to_vec(),
            min_time: Duration::from_millis(3),
            min_batches: 3,
            warmup_pairs: 2,
            batch_members: 65_536,
            max_batch_bytes: 32 * 1024 * 1024,
            memory_only: false,
        };
        let mut largest = 65_536;
        let mut positional = 0;
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "memory" || arg == "--memory" {
                config.memory_only = true;
                continue;
            }
            if arg == "--help" || arg == "-h" {
                println!(
                    "set_bench [samples=3] [largest=65536] [memory]\n\
                     --samples N --cardinalities N,N --widths N,N\n\
                     --operations NAME,NAME --min-ms N --min-batches N\n\
                     --warmup-pairs N --batch-members N --max-batch-mib N\n\
                     Explicit cardinalities override largest; widths must exceed 8.\n\
                     Timings exclude setup/destruction; snapshots keep both versions alive.\n\
                     Clone rows use ns/source member; snapshot mutations use ns/whole write.\n\
                     retain-sparse/iterate-sparse/clone-sparse use ns/surviving member.\n\
                     intersection/difference time retain+contains, excluding source copies.\n\
                     remove-churn is one remove+insert per member. hot-set-cycle has\n\
                     three hits, one miss, one duplicate insert, and remove+reinsert.\n\
                     Actual per-sample measured milliseconds and batch counts are reported."
                );
                println!(
                    "Operations: {}",
                    Operation::ALL
                        .iter()
                        .map(|operation| operation.name())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                std::process::exit(0);
            }
            if !arg.starts_with("--") {
                let number = arg.parse().expect("positional arguments must be integers");
                match positional {
                    0 => config.samples = number,
                    1 => largest = number,
                    _ => panic!("unexpected positional argument: {arg}"),
                }
                positional += 1;
                continue;
            }
            let (flag, inline) = arg
                .split_once('=')
                .map_or((arg.as_str(), None), |(a, b)| (a, Some(b.to_owned())));
            let value = inline
                .or_else(|| args.next())
                .unwrap_or_else(|| panic!("missing value for {flag}"));
            match flag {
                "--samples" => config.samples = value.parse().unwrap(),
                "--cardinalities" => config.cardinalities = parse_numbers(&value),
                "--widths" => config.widths = parse_numbers(&value),
                "--operations" => {
                    config.operations = value
                        .split(',')
                        .map(|name| {
                            let name = if name == "clone-snapshot" {
                                "clone-alone"
                            } else {
                                name
                            };
                            Operation::ALL
                                .iter()
                                .copied()
                                .find(|operation| operation.name() == name)
                                .unwrap_or_else(|| panic!("unknown operation: {name}"))
                        })
                        .collect();
                }
                "--min-ms" => {
                    config.min_time = Duration::from_millis(value.parse().unwrap());
                }
                "--min-batches" => config.min_batches = value.parse().unwrap(),
                "--warmup-pairs" => config.warmup_pairs = value.parse().unwrap(),
                "--batch-members" => config.batch_members = value.parse().unwrap(),
                "--max-batch-mib" => {
                    config.max_batch_bytes = value
                        .parse::<usize>()
                        .unwrap()
                        .checked_mul(1024 * 1024)
                        .expect("batch memory limit overflow");
                }
                _ => panic!("unknown flag: {flag}"),
            }
        }
        assert!(config.samples > 0 && largest > 0);
        assert!(config.min_batches > 0 && !config.min_time.is_zero());
        assert!(config.batch_members > 0 && config.max_batch_bytes > 0);
        if config.cardinalities.is_empty() {
            config.cardinalities = CARDINALITIES
                .into_iter()
                .filter(|count| *count <= largest)
                .collect();
        }
        assert!(config.cardinalities.iter().all(|count| *count > 0));
        assert!(!config.widths.is_empty() && config.widths.iter().all(|width| *width > 8));
        config
    }

    fn repetitions(&self, count: usize) -> usize {
        // Conservative allowance for entries, buckets, inline containers and
        // a transient realloc during snapshot SADD. Keep at least one result;
        // a single large table may exceed the requested batch budget.
        let table_allowance = count
            .checked_next_power_of_two()
            .and_then(|count| count.checked_mul(160))
            .and_then(|bytes| bytes.checked_add(256))
            .unwrap_or(usize::MAX);
        self.batch_members
            .div_ceil(count)
            .max(1)
            .min((self.max_batch_bytes / table_allowance).max(1))
            .min(MAX_BATCH_REPETITIONS)
    }
}

fn parse_numbers(value: &str) -> Vec<usize> {
    let mut numbers: Vec<_> = value
        .split(',')
        .map(|number| number.parse().expect("expected comma-separated integers"))
        .collect();
    numbers.sort_unstable();
    numbers.dedup();
    numbers
}

fn measure_pair(
    operation: Operation,
    members: &[Bytes],
    misses: &[Bytes],
    standard: &mut Prepared<StandardSet>,
    scannable: &mut Prepared<ScannableSet>,
    repetitions: usize,
    standard_first: bool,
) -> (Duration, Duration) {
    let mut standard_run =
        || measure_batch::<StandardSet>(operation, members, misses, standard, repetitions);
    let mut scannable_run =
        || measure_batch::<ScannableSet>(operation, members, misses, scannable, repetitions);
    if standard_first {
        (standard_run(), scannable_run())
    } else {
        let scannable = scannable_run();
        (standard_run(), scannable)
    }
}

fn print_timing(config: &Config, operation: Operation, members: &[Bytes], misses: &[Bytes]) {
    let count = members.len();
    let repetitions = config.repetitions(count);
    let units_per_batch = operation.units_per_repetition(count) * repetitions;
    let mut standard = Vec::with_capacity(config.samples);
    let mut scannable = Vec::with_capacity(config.samples);
    let mut paired_ratios = Vec::with_capacity(config.samples);
    let mut min_standard_time = Duration::MAX;
    let mut min_scannable_time = Duration::MAX;
    let mut total_pairs = 0usize;
    for sample in 0..config.samples {
        let seeds = Seeds::new();
        let mut standard_source = Prepared::<StandardSet>::new(operation, members, misses, &seeds);
        let mut scannable_source =
            Prepared::<ScannableSet>::new(operation, members, misses, &seeds);
        for warmup in 0..config.warmup_pairs {
            black_box(measure_pair(
                operation,
                members,
                misses,
                &mut standard_source,
                &mut scannable_source,
                repetitions,
                (sample + warmup) % 2 == 0,
            ));
        }
        let mut standard_time = Duration::ZERO;
        let mut scannable_time = Duration::ZERO;
        let mut pairs = 0usize;
        while pairs < config.min_batches
            || standard_time < config.min_time
            || scannable_time < config.min_time
        {
            let (a, b) = measure_pair(
                operation,
                members,
                misses,
                &mut standard_source,
                &mut scannable_source,
                repetitions,
                (sample + pairs) % 2 == 0,
            );
            standard_time += a;
            scannable_time += b;
            pairs += 1;
        }
        let units = units_per_batch as f64 * pairs as f64;
        let a = standard_time.as_nanos() as f64 / units;
        let b = scannable_time.as_nanos() as f64 / units;
        standard.push(a);
        scannable.push(b);
        paired_ratios.push(b / a);
        min_standard_time = min_standard_time.min(standard_time);
        min_scannable_time = min_scannable_time.min(scannable_time);
        total_pairs += pairs;
    }
    let a = median(&mut standard);
    let b = median(&mut scannable);
    let paired_ratio = median(&mut paired_ratios);
    println!(
        "{count},{},{},{a:.2},{b:.2},{:.3},{:.3},{:.2},{:.2},{:.2},{:.2},{:.3},{:.3},{total_pairs},{repetitions},{}",
        members[0].len(),
        operation.name(),
        b / a,
        paired_ratio,
        standard[0],
        standard[standard.len() - 1],
        scannable[0],
        scannable[scannable.len() - 1],
        min_standard_time.as_secs_f64() * 1000.0,
        min_scannable_time.as_secs_f64() * 1000.0,
        operation.unit(),
    );
}

fn main() {
    let config = Config::parse();
    println!("# Hashers: std HashSet uses SipHash; ScanSet uses aHash.");
    println!(
        "# samples={}, min_ms={}, min_batches={}, warmup_pairs={}, batch_members={}, max_batch_mib={}",
        config.samples,
        config.min_time.as_millis(),
        config.min_batches,
        config.warmup_pairs,
        config.batch_members,
        config.max_batch_bytes / (1024 * 1024),
    );
    println!("# Warm allocator and sources; shared Bytes before timing.");
    if !config.memory_only {
        println!(
            "members,member_bytes,operation,std_ns,scan_ns,scan_over_std,paired_median_ratio,std_min_ns,std_max_ns,scan_min_ns,scan_max_ns,std_min_timed_ms,scan_min_timed_ms,paired_batches,batch_repetitions,unit"
        );
        for &count in &config.cardinalities {
            for &width in &config.widths {
                let members = make_members(count, width, b'm');
                let misses = make_members(count, width, b'x');
                for &operation in &config.operations {
                    print_timing(&config, operation, &members, &misses);
                }
            }
        }
    }
    print_memory(&config.cardinalities);
}
