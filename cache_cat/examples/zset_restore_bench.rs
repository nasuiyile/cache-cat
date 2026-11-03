//! Measure ZSET decoding and operations on the restored layout.
//! Run: cargo run -p cache_cat --release --example zset_restore_bench -- [size] [trials]
//! Run this same example against each revision to compare restoration changes.
//! The input is one fixed bincode payload, prepared before timing. Restoration
//! excludes result checks and dropping the restored set; command timings exclude
//! request preparation and include normal response allocation and disposal.
//! These are in-memory measurements, excluding file I/O, locks and Raft.

use bytes::Bytes;
use cache_cat::protocol::zset::zadd::ZAddReq;
use cache_cat::raft::types::core::structure::sorted_set::{ScoreBound, SortedSet};
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::hint::black_box;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const OPERATIONS: [&str; 7] = [
    "ZRANK",
    "ZRANGE rank 10",
    "ZRANGE BYSCORE 10",
    "ZADD local update",
    "ZADD move score",
    "ZREM",
    "ZADD new middle",
];

struct Measurement {
    restore_ms: f64,
    heap_bytes: usize,
    operations: [f64; OPERATIONS.len()],
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

fn time(count: usize, mut operation: impl FnMut(usize)) -> f64 {
    let start = Instant::now();
    for index in 0..count {
        operation(index);
    }
    start.elapsed().as_secs_f64() * 1e9 / count as f64
}

fn measure(payload: &[u8], data: &[(Bytes, f64)], probes: &[usize]) -> Measurement {
    let size = data.len();
    let start = Instant::now();
    let mut set: SortedSet = bincode2::deserialize(black_box(payload)).unwrap();
    black_box(&set);
    let restore_ms = start.elapsed().as_secs_f64() * 1e3;
    assert_eq!(set.len(), size);
    // Sample payload sizes; every benchmark member has the same byte length.
    let heap_bytes = set.estimated_heap_usage(16);
    for &index in probes {
        assert_eq!(set.zrank(&data[index].0), Some(index as i64));
        assert_eq!(set.zscore(&data[index].0), Some(data[index].1));
    }

    let rank = time(probes.len(), |index| {
        black_box(set.zrank(black_box(&data[probes[index]].0)));
    });
    let rank_range = time(probes.len(), |index| {
        let start = probes[index] as i64;
        black_box(set.zrange_rank(black_box(start), black_box(start + 9), false));
    });
    let score_bounds: Vec<_> = probes
        .iter()
        .map(|&index| {
            let score = data[index].1;
            (
                ScoreBound {
                    value: score,
                    exclusive: false,
                },
                ScoreBound {
                    value: score + 3.0,
                    exclusive: false,
                },
            )
        })
        .collect();
    let score_range = time(probes.len(), |index| {
        let (min, max) = &score_bounds[index];
        black_box(set.zrange_score(black_box(min), black_box(max), false, Some((0, 10))));
    });

    // Reverse member order within tied scores keeps each +0.25 update in its
    // current sorted position, exercising updates that need no block move.
    let mut local_requests = data
        .iter()
        .rev()
        .map(|(member, score)| request(member.clone(), *score + 0.25))
        .collect::<Vec<_>>()
        .into_iter();
    let local_update = time(size, |_| {
        black_box(set.zadd(local_requests.next().unwrap()));
    });
    assert_eq!(set.len(), size);

    // Move members in the same deterministic shuffled order in both revisions.
    let mut moved_requests = probes
        .iter()
        .map(|&index| request(data[index].0.clone(), -data[index].1 - 1.0))
        .collect::<Vec<_>>()
        .into_iter();
    let moved_update = time(probes.len(), |_| {
        black_box(set.zadd(moved_requests.next().unwrap()));
    });
    for &index in probes {
        assert_eq!(set.zscore(&data[index].0), Some(-data[index].1 - 1.0));
    }
    let remove = time(probes.len(), |index| {
        black_box(set.zrem(std::slice::from_ref(black_box(&data[probes[index]].0))));
    });
    assert_eq!(set.len(), size - probes.len());

    // Restore a fresh layout so inserts exercise its original block occupancy.
    let mut inserted: SortedSet = bincode2::deserialize(payload).unwrap();
    let mut insert_indices: Vec<_> = (0..size).collect();
    insert_indices.shuffle(&mut StdRng::seed_from_u64(0x696e_7365_7274_5f6d));
    insert_indices.truncate(size.min(100_000));
    let insert_count = insert_indices.len();
    let mut insert_requests = insert_indices
        .into_iter()
        .map(|index| request(Bytes::from(format!("new:{index:020}")), data[index].1 + 0.5))
        .collect::<Vec<_>>()
        .into_iter();
    let middle_insert = time(insert_count, |_| {
        black_box(inserted.zadd(insert_requests.next().unwrap()));
    });
    assert_eq!(inserted.len(), size + insert_count);

    Measurement {
        restore_ms,
        heap_bytes,
        operations: [
            rank,
            rank_range,
            score_range,
            local_update,
            moved_update,
            remove,
            middle_insert,
        ],
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run(size: usize, trials: usize) {
    let data: Vec<_> = (0..size)
        .map(|index| {
            (
                Bytes::from(format!("member:{index:020}")),
                (index / 4) as f64,
            )
        })
        .collect();
    // SortedSet serializes exactly this sequence of (member, score) tuples.
    // Constructing it directly keeps the fixture independent of the builder.
    let payload = bincode2::serialize(&data).unwrap();
    let mut probes: Vec<_> = (0..size).collect();
    probes.shuffle(&mut StdRng::seed_from_u64(0x7365_745f_7265_7374));
    probes.truncate(size.min(10_000));

    // Warm both decoding and the operations before collecting samples.
    black_box(measure(&payload, &data, &probes));
    let samples: Vec<_> = (0..trials)
        .map(|_| measure(&payload, &data, &probes))
        .collect();
    let restore_ms = median(samples.iter().map(|sample| sample.restore_ms).collect());
    let heap_bytes = median(
        samples
            .iter()
            .map(|sample| sample.heap_bytes as f64)
            .collect(),
    );
    println!(
        "\nN={size}, payload={} bytes, {trials} trials, medians",
        payload.len()
    );
    println!(
        "restore: {restore_ms:.3} ms, {:.1} ns/member, {:.1} MiB/s",
        restore_ms * 1e6 / size as f64,
        payload.len() as f64 / (1024.0 * 1024.0) / (restore_ms / 1e3)
    );
    println!("restored estimated heap: {heap_bytes:.0} bytes");
    for (index, name) in OPERATIONS.iter().enumerate() {
        let ns = median(
            samples
                .iter()
                .map(|sample| sample.operations[index])
                .collect(),
        );
        println!("{name:<22} {ns:>12.1} ns/op");
    }
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert!(args.len() <= 2, "usage: zset_restore_bench [size] [trials]");
    let trials = args
        .get(1)
        .map(|value| value.parse::<usize>().expect("trials must be an integer"))
        .unwrap_or(5);
    assert!(trials > 0, "trials must be positive");
    let sizes = match args.first() {
        Some(value) => vec![value.parse::<usize>().expect("size must be an integer")],
        None => vec![1_000, 10_000, 100_000],
    };
    for size in sizes {
        assert!(
            size > 0 && size <= i64::MAX as usize - 10,
            "size is out of range"
        );
        run(size, trials);
    }
}
