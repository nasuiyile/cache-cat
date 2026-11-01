//! Compare the indexed skip list against the previous BTreeSet + HashMap layout.
//! Run with: cargo run -p cache_cat --release --example zset_bench -- [size] [trials]
//! This measures in-memory operations, including member cloning and the same
//! request allocation on both sides; it excludes networking, locks and Raft.

use bytes::Bytes;
use cache_cat::protocol::zset::zadd::ZAddReq;
use cache_cat::raft::types::core::structure::sorted_set::{ScoreBound, SortedSet};
use ordered_float::OrderedFloat;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::collections::{BTreeSet, HashMap};
use std::hint::black_box;
use std::time::Instant;

// Match the server's allocator for both layouts, including node/tower storage.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

trait Workload: Default {
    fn add(&mut self, request: ZAddReq) -> i64;
    fn remove(&mut self, member: &Bytes) -> i64;
    fn score(&self, member: &Bytes) -> Option<f64>;
    fn rank(&self, member: &Bytes) -> Option<i64>;
    fn count(&self, min: f64, max: f64) -> i64;
    fn tail(&self, size: usize) -> Vec<(Bytes, f64)>;
    fn by_score(&self, min: f64, max: f64) -> Vec<(Bytes, f64)>;
}

impl Workload for SortedSet {
    fn add(&mut self, request: ZAddReq) -> i64 {
        self.zadd(request)
    }
    fn remove(&mut self, member: &Bytes) -> i64 {
        self.zrem(std::slice::from_ref(member))
    }
    fn score(&self, member: &Bytes) -> Option<f64> {
        self.zscore(member)
    }
    fn rank(&self, member: &Bytes) -> Option<i64> {
        self.zrank(member)
    }
    fn count(&self, min: f64, max: f64) -> i64 {
        self.zcount(min, max, false, false)
    }
    fn tail(&self, _size: usize) -> Vec<(Bytes, f64)> {
        self.zrange(-10, -1)
    }
    fn by_score(&self, min: f64, max: f64) -> Vec<(Bytes, f64)> {
        self.zrange_score(
            &ScoreBound {
                value: min,
                exclusive: false,
            },
            &ScoreBound {
                value: max,
                exclusive: false,
            },
            false,
            Some((0, 10)),
        )
    }
}

#[derive(Default)]
struct PreviousSet {
    tree: BTreeSet<(OrderedFloat<f64>, Bytes)>,
    hash: HashMap<Bytes, f64>,
}

impl Workload for PreviousSet {
    fn add(&mut self, request: ZAddReq) -> i64 {
        let mut added = 0;
        for (member, score) in request.members {
            if let Some(old) = self.hash.get(&member).copied() {
                if old != score {
                    self.tree.remove(&(OrderedFloat(old), member.clone()));
                    self.tree.insert((OrderedFloat(score), member.clone()));
                    self.hash.insert(member, score);
                }
            } else {
                self.tree.insert((OrderedFloat(score), member.clone()));
                self.hash.insert(member, score);
                added += 1;
            }
        }
        added
    }

    fn remove(&mut self, member: &Bytes) -> i64 {
        if let Some(score) = self.hash.remove(member) {
            self.tree.remove(&(OrderedFloat(score), member.clone()));
            1
        } else {
            0
        }
    }

    fn score(&self, member: &Bytes) -> Option<f64> {
        self.hash.get(member).copied()
    }

    fn rank(&self, member: &Bytes) -> Option<i64> {
        let score = self.hash.get(member).copied()?;
        let key = (OrderedFloat(score), member.clone());
        self.tree
            .iter()
            .position(|entry| entry == &key)
            .map(|rank| rank as i64)
    }

    fn count(&self, min: f64, max: f64) -> i64 {
        self.tree
            .iter()
            .filter(|(score, _)| score.0 >= min && score.0 <= max)
            .count() as i64
    }

    fn tail(&self, size: usize) -> Vec<(Bytes, f64)> {
        self.tree
            .iter()
            .skip(size.saturating_sub(10))
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }
    fn by_score(&self, min: f64, max: f64) -> Vec<(Bytes, f64)> {
        self.tree
            .range((OrderedFloat(min), Bytes::new())..)
            .take_while(|(score, _)| score.0 <= max)
            .take(10)
            .map(|(score, member)| (member.clone(), score.0))
            .collect()
    }
}

fn request(member: Bytes, score: f64) -> ZAddReq {
    ZAddReq {
        key: Bytes::new(),
        nx: false,
        xx: false,
        gt: false,
        lt: false,
        ch: false,
        members: vec![(member, score)],
    }
}

const OPERATIONS: [&str; 9] = [
    "ZADD insert",
    "ZSCORE",
    "ZRANK",
    "ZCOUNT",
    "ZRANGE last 10",
    "ZRANGE BYSCORE 10",
    "ZADD local update",
    "ZADD move score",
    "ZREM",
];

fn time(count: usize, mut operation: impl FnMut(usize)) -> f64 {
    let start = Instant::now();
    for i in 0..count {
        operation(i);
    }
    start.elapsed().as_secs_f64() * 1e9 / count as f64
}

fn measure<T: Workload>(data: &[(Bytes, f64)], probes: &[usize]) -> [f64; 9] {
    let size = data.len();
    let mut set = T::default();
    let insert = time(size, |i| {
        let (member, score) = &data[i];
        black_box(set.add(request(member.clone(), *score)));
    });
    let score = time(size * 10, |i| {
        black_box(set.score(black_box(&data[i % size].0)));
    });
    let rank = time(probes.len(), |i| {
        black_box(set.rank(black_box(&data[probes[i]].0)));
    });
    let count = time(probes.len(), |i| {
        let bound = data[probes[i]].1;
        black_box(set.count(black_box(bound), black_box(bound + 100.0)));
    });
    let tail = time(probes.len(), |_| {
        black_box(set.tail(black_box(size)));
    });
    let by_score = time(probes.len(), |i| {
        let bound = data[probes[i]].1;
        black_box(set.by_score(black_box(bound), black_box(bound + 10.0)));
    });
    // Update equal-score groups from the largest member down. A small score
    // increase then preserves every member's position, including tied scores.
    // Sorting the workload is preparation and is excluded from the timings.
    let mut local_order: Vec<_> = (0..size).collect();
    local_order.sort_unstable_by(|&a, &b| {
        data[b]
            .1
            .total_cmp(&data[a].1)
            .then_with(|| data[b].0.cmp(&data[a].0))
    });
    let local_update = time(size, |i| {
        let (member, score) = &data[local_order[i]];
        black_box(set.add(request(member.clone(), *score + 0.25)));
    });
    let update = time(size, |i| {
        let (member, score) = &data[i];
        black_box(set.add(request(member.clone(), -*score - 1.0)));
    });
    let delete = time(size, |i| {
        black_box(set.remove(black_box(&data[i].0)));
    });
    [
        insert,
        score,
        rank,
        count,
        tail,
        by_score,
        local_update,
        update,
        delete,
    ]
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run(size: usize, trials: usize) {
    let mut rng = StdRng::seed_from_u64(0x7a_7365_7420_6265_6e);
    let mut data: Vec<_> = (0..size)
        .map(|i| (Bytes::from(format!("member:{i:09}")), (i / 4) as f64))
        .collect();
    data.shuffle(&mut rng);
    let mut probes: Vec<_> = (0..size).collect();
    probes.shuffle(&mut rng);
    probes.truncate(size.min(300));
    let mut old = Vec::with_capacity(trials);
    let mut new = Vec::with_capacity(trials);
    for trial in 0..trials {
        if trial % 2 == 0 {
            old.push(measure::<PreviousSet>(&data, &probes));
            new.push(measure::<SortedSet>(&data, &probes));
        } else {
            new.push(measure::<SortedSet>(&data, &probes));
            old.push(measure::<PreviousSet>(&data, &probes));
        }
    }
    println!("\nN={size}, {trials} trials, median nanoseconds per operation");
    println!(
        "{:<18} {:>14} {:>14} {:>13}",
        "operation", "BTree+HashMap", "skiplist+hash", "new / old"
    );
    for (i, name) in OPERATIONS.iter().enumerate() {
        let previous = median(&mut old.iter().map(|row| row[i]).collect::<Vec<_>>());
        let current = median(&mut new.iter().map(|row| row[i]).collect::<Vec<_>>());
        println!(
            "{name:<18} {previous:>14.1} {current:>14.1} {:>12.3}x",
            current / previous
        );
    }
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let trials = args
        .get(1)
        .map(|v| v.parse::<usize>().expect("trials must be an integer"))
        .unwrap_or(5);
    assert!(trials > 0, "trials must be positive");
    let sizes = if let Some(size) = args.first() {
        vec![size.parse::<usize>().expect("size must be an integer")]
    } else {
        vec![1_000, 10_000, 100_000]
    };
    for size in sizes {
        assert!(
            size > 0 && size <= usize::MAX / 10,
            "size must be positive and fit the probe count"
        );
        run(size, trials);
    }
}
