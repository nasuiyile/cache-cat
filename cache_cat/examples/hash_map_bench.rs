//! Compare Redis Hash storage operations with the former standard HashMap.
//!
//! Run with `cargo run -p cache_cat --release --example hash_map_bench`.
//! Optional arguments: sample count (default 7), largest cardinality (default 65536).
//! Add a third argument `memory` to report table allocations instead of timings.
//! Reports median nanoseconds per field operation. Keys and map setup are outside
//! timed regions; each map uses its default randomized hasher and real Bytes/HashValue
//! payloads (std SipHash versus ScanMap aHash). Seeds are cloned per sample so
//! repeated map construction uses the same placement. Most rows isolate container
//! cost; hget-reply also includes the
//! Hash mutex and Redis string-value conversion. RESP encoding and Raft are excluded.

use bytes::Bytes;
use cache_cat::raft::types::core::structure::scan_map::ScanMap;
use cache_cat::raft::types::core::value_object::HashValue;
use std::collections::{HashMap, hash_map::RandomState};
use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type StandardMap = HashMap<Bytes, HashValue>;
type ScannableMap = ScanMap<Bytes, HashValue>;

#[derive(Clone)]
struct Hashers {
    standard: RandomState,
    scannable: ahash::RandomState,
}

trait Map: Sized {
    fn empty(capacity: usize, state: Hashers) -> Self;
    fn get(&self, key: &Bytes) -> Option<&HashValue>;
    fn insert(&mut self, key: Bytes, value: HashValue) -> Option<HashValue>;
    fn remove(&mut self, key: &Bytes) -> Option<HashValue>;
    fn visit(&self) -> usize;
}

macro_rules! impl_map {
    ($map:ty, $hasher:ident) => {
        impl Map for $map {
            fn empty(capacity: usize, state: Hashers) -> Self {
                Self::with_capacity_and_hasher(capacity, state.$hasher)
            }
            fn get(&self, key: &Bytes) -> Option<&HashValue> {
                Self::get(self, key)
            }
            fn insert(&mut self, key: Bytes, value: HashValue) -> Option<HashValue> {
                Self::insert(self, key, value)
            }
            fn remove(&mut self, key: &Bytes) -> Option<HashValue> {
                Self::remove(self, key)
            }
            fn visit(&self) -> usize {
                self.iter().fold(0usize, |sum, (key, value)| {
                    black_box(value);
                    sum.wrapping_add(black_box(key).len())
                })
            }
        }
    };
}
impl_map!(StandardMap, standard);
impl_map!(ScannableMap, scannable);

#[derive(Clone, Copy)]
enum Operation {
    GetHit,
    GetMiss,
    GetReply,
    Update,
    Delete,
    InsertReserved,
    InsertGrowing,
    Iterate,
}
impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::GetHit => "get-hit",
            Self::GetMiss => "get-miss",
            Self::GetReply => "hget-reply",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::InsertReserved => "insert-reserved",
            Self::InsertGrowing => "insert-growing",
            Self::Iterate => "iterate",
        }
    }
}

fn fill<M: Map>(keys: &[Bytes], values: &[HashValue], state: Hashers) -> M {
    let mut map = M::empty(keys.len(), state);
    // All configured sizes are coprime to 53. Insert in a different order
    // from the lookup/removal sequence to avoid favoring contiguous entries.
    for offset in 0..keys.len() {
        let index = (offset * 53) % keys.len();
        map.insert(keys[index].clone(), values[index].clone());
    }
    map
}

fn measure<M: Map>(
    operation: Operation,
    keys: &[Bytes],
    misses: &[Bytes],
    values: &[HashValue],
    state: Hashers,
) -> f64 {
    let count = keys.len();
    let loops = (1_048_576 / count).max(1);
    match operation {
        Operation::GetReply => {
            let map = parking_lot::Mutex::new(fill::<M>(keys, values, state));
            let start = Instant::now();
            for _ in 0..loops {
                for key in keys {
                    let reply = {
                        let guard = map.lock();
                        match guard.get(black_box(key)) {
                            Some(HashValue::Str(value)) => Some(value.clone()),
                            Some(HashValue::Int(value)) => Some(Bytes::from(value.to_string())),
                            None => None,
                        }
                    };
                    black_box(reply);
                }
            }
            let elapsed = start.elapsed();
            black_box(&map);
            elapsed.as_nanos() as f64 / (count * loops) as f64
        }
        Operation::InsertReserved | Operation::InsertGrowing => {
            // Batch small independent hashes so timer granularity does not
            // dominate. Allocate only the empty containers before timing.
            let capacity = if matches!(operation, Operation::InsertReserved) {
                count
            } else {
                0
            };
            let mut maps: Vec<_> = (0..loops)
                .map(|_| M::empty(capacity, state.clone()))
                .collect();
            let start = Instant::now();
            for map in &mut maps {
                for (key, value) in keys.iter().zip(values) {
                    black_box(map.insert(black_box(key).clone(), value.clone()));
                }
            }
            let elapsed = start.elapsed();
            black_box(&maps);
            elapsed.as_nanos() as f64 / (count * loops) as f64
        }
        Operation::Delete => {
            let mut maps: Vec<_> = (0..loops)
                .map(|_| fill::<M>(keys, values, state.clone()))
                .collect();
            let start = Instant::now();
            for map in &mut maps {
                for key in keys {
                    black_box(map.remove(black_box(key)));
                }
            }
            let elapsed = start.elapsed();
            black_box(&maps);
            elapsed.as_nanos() as f64 / (count * loops) as f64
        }
        _ => {
            let mut map = fill::<M>(keys, values, state);
            let start = Instant::now();
            for _ in 0..loops {
                match operation {
                    Operation::GetHit => {
                        for key in keys {
                            black_box(map.get(black_box(key)));
                        }
                    }
                    Operation::GetMiss => {
                        for key in misses {
                            black_box(map.get(black_box(key)));
                        }
                    }
                    Operation::Update => {
                        for (key, value) in keys.iter().zip(values) {
                            black_box(map.insert(black_box(key).clone(), value.clone()));
                        }
                    }
                    Operation::Iterate => {
                        black_box(black_box(&map).visit());
                    }
                    _ => unreachable!(),
                }
            }
            let elapsed = start.elapsed();
            black_box(&map);
            elapsed.as_nanos() as f64 / (count * loops) as f64
        }
    }
}

fn median(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn keys(count: usize, width: usize, prefix: u8) -> Vec<Bytes> {
    // Shuffle the request order deterministically. Sequential field IDs would
    // otherwise favor the dense entry vector's insertion order on lookup.
    let mut result: Vec<_> = (0..count)
        .map(|index| {
            let mut key = vec![prefix; width];
            key[width - 8..].copy_from_slice(&(index as u64).to_le_bytes());
            Bytes::from(key)
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

fn print_memory(largest: usize) {
    println!(
        "fields,std_table_estimated_bytes,scan_reserved_table_bytes,scan_grown_table_bytes,std_inline_bytes,scan_inline_bytes"
    );
    for count in [1, 4, 8, 64, 1024, 10_000, 65_536, 262_144] {
        if count > largest {
            continue;
        }
        let standard = StandardMap::with_capacity(count);
        let scannable = ScannableMap::with_capacity(count);
        let mut grown = ScannableMap::new();
        for index in 0..count {
            grown.insert(
                Bytes::copy_from_slice(&index.to_le_bytes()),
                HashValue::Int(index as i64),
            );
        }
        // Same SwissTable estimate as MEMORY USAGE. The standard library does
        // not expose allocation bytes. Excludes allocator overhead and payloads.
        let capacity = standard.capacity();
        let buckets = if capacity < 4 {
            4
        } else if capacity < 8 {
            8
        } else {
            ((capacity * 8 + 6) / 7).next_power_of_two()
        };
        let data = buckets * size_of::<(Bytes, HashValue)>();
        let estimate = data + (16 - data % 16) % 16 + buckets + 16;
        println!(
            "{count},{estimate},{},{},{},{}",
            scannable.allocated_bytes(),
            grown.allocated_bytes(),
            size_of::<StandardMap>(),
            size_of::<ScannableMap>()
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let samples: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(7);
    let largest: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(65_536);
    assert!(samples > 0);
    if args.next().as_deref() == Some("memory") {
        print_memory(largest);
        return;
    }
    println!("fields,key_bytes,operation,std_ns,scan_ns,scan_over_std");
    for count in [1, 4, 8, 64, 1024, 10_000, 65_536, 262_144] {
        if count > largest {
            continue;
        }
        for width in [12, 96] {
            let fields = keys(count, width, b'f');
            let misses = keys(count, width, b'm');
            let values: Vec<_> = (0..count)
                .map(|index| {
                    if index % 2 == 0 {
                        HashValue::Int(index as i64)
                    } else {
                        HashValue::Str(Bytes::from(vec![b'v'; 32]))
                    }
                })
                .collect();
            for operation in [
                Operation::GetHit,
                Operation::GetMiss,
                Operation::GetReply,
                Operation::Update,
                Operation::Delete,
                Operation::InsertReserved,
                Operation::InsertGrowing,
                Operation::Iterate,
            ] {
                let mut standard = Vec::with_capacity(samples);
                let mut scannable = Vec::with_capacity(samples);
                // The first pair warms the code/data paths and is discarded.
                for sample in 0..=samples {
                    let seed = Hashers {
                        standard: RandomState::new(),
                        scannable: ahash::RandomState::new(),
                    };
                    let standard_run = || {
                        measure::<StandardMap>(operation, &fields, &misses, &values, seed.clone())
                    };
                    let scannable_run = || {
                        measure::<ScannableMap>(operation, &fields, &misses, &values, seed.clone())
                    };
                    let (a, b) = if sample % 2 == 0 {
                        (standard_run(), scannable_run())
                    } else {
                        let b = scannable_run();
                        (standard_run(), b)
                    };
                    if sample != 0 {
                        standard.push(a);
                        scannable.push(b);
                    }
                }
                let a = median(&mut standard);
                let b = median(&mut scannable);
                println!(
                    "{count},{width},{},{a:.2},{b:.2},{:.3}",
                    operation.name(),
                    b / a
                );
            }
        }
    }
}
