use crate::protocol::zset::zadd::ZAddReq;
use crate::raft::types::core::structure::sorted_set::{LexBound, ScoreBound, SortedSet};
use bytes::Bytes;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;

fn request(members: Vec<(Bytes, f64)>) -> ZAddReq {
    ZAddReq {
        key: Bytes::from_static(b"z"),
        nx: false,
        xx: false,
        gt: false,
        lt: false,
        ch: false,
        members,
    }
}

fn sorted(model: &HashMap<Bytes, f64>) -> Vec<(Bytes, f64)> {
    let mut values: Vec<_> = model.iter().map(|(m, s)| (m.clone(), *s)).collect();
    values.sort_by(|(a, x), (b, y)| x.partial_cmp(y).unwrap().then_with(|| a.cmp(b)));
    values
}

fn verify(set: &SortedSet, model: &HashMap<Bytes, f64>) {
    let values = sorted(model);
    assert_eq!(set.len(), values.len());
    assert_eq!(set.is_empty(), values.is_empty());
    assert_eq!(set.zrange(0, -1), values);
    let reverse: Vec<_> = values.iter().rev().cloned().collect();
    assert_eq!(set.zrange_rank(0, -1, true), reverse);
    for (rank, (member, score)) in values.iter().enumerate() {
        assert_eq!(set.zscore(member), Some(*score));
        assert_eq!(set.zrank(member), Some(rank as i64));
        assert_eq!(set.zrevrank(member), Some((values.len() - rank - 1) as i64));
    }
    let missing = Bytes::from_static(b"not-in-the-model");
    assert_eq!(set.zscore(&missing), None);
    assert_eq!(set.zrank(&missing), None);
    assert_eq!(set.zrevrank(&missing), None);
    for (min, max, min_exclusive, max_exclusive) in [
        (f64::NEG_INFINITY, f64::INFINITY, false, false),
        (-10.0, 10.0, true, false),
        (0.0, 0.0, false, false),
        (0.0, 0.0, true, false),
        (20.0, -20.0, false, false),
    ] {
        let expected = values
            .iter()
            .filter(|(_, score)| {
                (*score > min || !min_exclusive && *score == min)
                    && (*score < max || !max_exclusive && *score == max)
            })
            .count();
        assert_eq!(
            set.zcount(min, max, min_exclusive, max_exclusive),
            expected as i64
        );
    }
}

#[test]
fn mixed_writes_preserve_scores_order_and_ranks() {
    let mut set = SortedSet::new();
    let mut model = HashMap::new();
    let mut rng = StdRng::seed_from_u64(0x7a_7365_7420_7261_6e);
    for step in 0..6000 {
        let member = Bytes::from(format!("member:{:03}", rng.gen_range(0..257)));
        match rng.gen_range(0..10) {
            0..=4 => {
                let score = rng.gen_range(-40..=40) as f64 / 2.0;
                let old = model.insert(member.clone(), score);
                let mut req = request(vec![(member, score)]);
                req.ch = step % 2 == 0;
                let expected = if req.ch {
                    old != Some(score)
                } else {
                    old.is_none()
                };
                assert_eq!(set.zadd(req), i64::from(expected));
            }
            5..=6 => {
                let increment = rng.gen_range(-8..=8) as f64 / 2.0;
                let score = model.entry(member.clone()).or_insert(0.0);
                *score += increment;
                assert_eq!(set.zincrby(member, increment), Some(*score));
            }
            7..=8 => {
                let removed = model.remove(&member).is_some();
                assert_eq!(set.zrem(&[member.clone(), member]), i64::from(removed));
            }
            _ => {
                let count = rng.gen_range(0..=5);
                let expected: Vec<_> = sorted(&model).into_iter().take(count).collect();
                for (member, _) in &expected {
                    model.remove(member);
                }
                assert_eq!(set.zpop_min(Some(count)), expected);
            }
        }
        if step % 31 == 0 {
            verify(&set, &model);
        }
    }
    verify(&set, &model);
    assert_eq!(set.zpop_min(Some(usize::MAX)), sorted(&model));
    assert!(set.is_empty());
    assert!(set.zpop_min(None).is_empty());
    // Reuse a collection after all nodes have been removed.
    set.zadd(request(vec![(Bytes::new(), 1.0)]));
    assert_eq!(set.zrank(&Bytes::new()), Some(0));
    assert_eq!(set.zpop_min(None), vec![(Bytes::new(), 1.0)]);
}

#[test]
fn conditional_adds_and_duplicate_members_keep_redis_counts() {
    let a = Bytes::from_static(b"a");
    let b = Bytes::from_static(b"b");
    let c = Bytes::from_static(b"c");
    let mut set = SortedSet::new();
    assert_eq!(
        set.zadd(request(vec![(a.clone(), 1.0), (a.clone(), 2.0)])),
        1
    );
    assert_eq!(set.zscore(&a), Some(2.0));

    let mut nx = request(vec![(a.clone(), 9.0), (b.clone(), 3.0)]);
    nx.nx = true;
    nx.ch = true;
    assert_eq!(set.zadd(nx), 1);
    assert_eq!(set.zscore(&a), Some(2.0));

    let mut xx = request(vec![(a.clone(), 4.0), (c.clone(), 5.0)]);
    xx.xx = true;
    xx.ch = true;
    assert_eq!(set.zadd(xx), 1);
    assert_eq!(set.zscore(&c), None);

    let mut gt = request(vec![(a.clone(), 3.0), (b.clone(), 6.0), (c.clone(), -1.0)]);
    gt.gt = true;
    gt.ch = true;
    assert_eq!(set.zadd(gt), 2);
    assert_eq!(set.zscore(&a), Some(4.0));

    let mut lt = request(vec![(a.clone(), 4.0), (b.clone(), 2.0), (c.clone(), 5.0)]);
    lt.lt = true;
    lt.ch = true;
    assert_eq!(set.zadd(lt), 1);
    assert_eq!(set.zscore(&c), Some(-1.0));

    let mut changes = request(vec![(a.clone(), 7.0), (a.clone(), 8.0), (a.clone(), 8.0)]);
    changes.ch = true;
    assert_eq!(set.zadd(changes), 2);
    verify(&set, &HashMap::from([(a, 8.0), (b, 2.0), (c, -1.0)]));
}

#[test]
fn rank_ranges_handle_negative_extreme_and_reverse_indices() {
    let mut set = SortedSet::new();
    let values: Vec<_> = (0..17)
        .map(|i| (Bytes::from(format!("m{i:02}")), i as f64))
        .collect();
    set.zadd(request(values.clone()));
    let indices = [
        i64::MIN,
        -30,
        -18,
        -17,
        -3,
        -1,
        0,
        1,
        8,
        16,
        17,
        30,
        i64::MAX,
    ];
    for reverse in [false, true] {
        let mut ordered = values.clone();
        if reverse {
            ordered.reverse();
        }
        for start in indices {
            for stop in indices {
                let normalize = |index: i64| {
                    if index < 0 {
                        ordered.len() as i128 + index as i128
                    } else {
                        index as i128
                    }
                };
                let expected: Vec<_> = ordered
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| {
                        *i as i128 >= normalize(start) && *i as i128 <= normalize(stop)
                    })
                    .map(|(_, value)| value.clone())
                    .collect();
                assert_eq!(
                    set.zrange_rank(start, stop, reverse),
                    expected,
                    "start={start}, stop={stop}, reverse={reverse}"
                );
            }
        }
    }
}

fn limited<T: Clone>(values: &[T], limit: Option<(i64, i64)>) -> Vec<T> {
    match limit {
        None => values.to_vec(),
        Some((offset, _)) if offset < 0 => Vec::new(),
        Some((offset, count)) => values
            .iter()
            .skip(offset as usize)
            .take(if count < 0 {
                usize::MAX
            } else {
                count as usize
            })
            .cloned()
            .collect(),
    }
}

#[test]
fn score_ranges_count_and_limits_include_binary_ties_and_infinities() {
    let values = vec![
        (Bytes::from_static(b"minus"), f64::NEG_INFINITY),
        (Bytes::from_static(b"negative"), -1.0),
        (Bytes::new(), -0.0),
        (Bytes::from_static(b"\0"), 0.0),
        (Bytes::from_static(b"a"), 0.0),
        (Bytes::from_static(b"a\0"), 0.0),
        (Bytes::from_static(b"\xff"), 0.0),
        (Bytes::from_static(b"positive"), 1.0),
        (Bytes::from_static(b"plus"), f64::INFINITY),
    ];
    let mut set = SortedSet::new();
    set.zadd(request(values.clone()));
    let boundaries = [f64::NEG_INFINITY, -2.0, -1.0, 0.0, 0.5, 1.0, f64::INFINITY];
    let limits = [
        None,
        Some((0, 0)),
        Some((1, 2)),
        Some((2, -1)),
        Some((1, -2)),
        Some((-1, 3)),
        Some((i64::MAX, 1)),
    ];
    for min in boundaries {
        for max in boundaries {
            for min_exclusive in [false, true] {
                for max_exclusive in [false, true] {
                    let lower = ScoreBound {
                        value: min,
                        exclusive: min_exclusive,
                    };
                    let upper = ScoreBound {
                        value: max,
                        exclusive: max_exclusive,
                    };
                    let expected: Vec<_> = values
                        .iter()
                        .filter(|(_, score)| {
                            (*score > min || !min_exclusive && *score == min)
                                && (*score < max || !max_exclusive && *score == max)
                        })
                        .cloned()
                        .collect();
                    assert_eq!(
                        set.zcount(min, max, min_exclusive, max_exclusive),
                        expected.len() as i64
                    );
                    for limit in limits {
                        assert_eq!(
                            set.zrange_score(&lower, &upper, false, limit),
                            limited(&expected, limit)
                        );
                        let reverse: Vec<_> = expected.iter().rev().cloned().collect();
                        assert_eq!(
                            set.zrange_score(&upper, &lower, true, limit),
                            limited(&reverse, limit)
                        );
                    }
                    if !min_exclusive && !max_exclusive {
                        assert_eq!(
                            set.zrangebyscore(min, max, Some((1, 3))),
                            limited(&expected, Some((1, 3)))
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn equal_score_lex_ranges_compare_raw_bytes_in_both_directions() {
    let members: Vec<_> = [b"".as_slice(), b"\0", b"a", b"a\0", b"a\xff", b"b", b"\xff"]
        .into_iter()
        .map(Bytes::copy_from_slice)
        .collect();
    let values: Vec<_> = members
        .iter()
        .cloned()
        .map(|member| (member, 5.0))
        .collect();
    let mut set = SortedSet::new();
    set.zadd(request(values.clone()));
    let mut bounds = vec![LexBound::NegativeInfinity, LexBound::PositiveInfinity];
    for member in members {
        for exclusive in [false, true] {
            bounds.push(LexBound::Value {
                value: member.clone(),
                exclusive,
            });
        }
    }
    for lower in &bounds {
        for upper in &bounds {
            let expected: Vec<_> = values
                .iter()
                .filter(|(member, _)| {
                    let above = match lower {
                        LexBound::NegativeInfinity => true,
                        LexBound::PositiveInfinity => false,
                        LexBound::Value { value, exclusive } => {
                            member > value || !exclusive && member == value
                        }
                    };
                    let below = match upper {
                        LexBound::NegativeInfinity => false,
                        LexBound::PositiveInfinity => true,
                        LexBound::Value { value, exclusive } => {
                            member < value || !exclusive && member == value
                        }
                    };
                    above && below
                })
                .cloned()
                .collect();
            for limit in [
                None,
                Some((1, 2)),
                Some((2, -1)),
                Some((-1, 2)),
                Some((0, 0)),
            ] {
                assert_eq!(
                    set.zrange_lex(lower, upper, false, limit),
                    limited(&expected, limit)
                );
                let reverse: Vec<_> = expected.iter().rev().cloned().collect();
                assert_eq!(
                    set.zrange_lex(upper, lower, true, limit),
                    limited(&reverse, limit)
                );
            }
        }
    }
}

#[test]
fn clone_and_snapshot_restore_remain_independent_and_mutable_after_churn() {
    let mut set = SortedSet::new();
    let mut model: HashMap<Bytes, f64> = (0..300)
        .map(|i| (Bytes::from(format!("member:{i}")), (i % 7) as f64))
        .collect();
    set.zadd(request(
        model.iter().map(|(m, s)| (m.clone(), *s)).collect(),
    ));
    for i in (0..300).step_by(3) {
        let member = Bytes::from(format!("member:{i}"));
        model.remove(&member);
        set.zrem(&[member]);
    }
    let serialized = bincode2::serialize(&set).unwrap();
    let restored: SortedSet = bincode2::deserialize(&serialized).unwrap();
    for mut copy in [set.clone(), restored] {
        verify(&copy, &model);
        let mut expected = model.clone();
        for i in 0..400 {
            let member = Bytes::from(format!("member:{i}"));
            let score = (400 - i) as f64;
            expected.insert(member.clone(), score);
            copy.zadd(request(vec![(member, score)]));
        }
        let popped: Vec<_> = sorted(&expected).into_iter().take(37).collect();
        for (member, _) in &popped {
            expected.remove(member);
        }
        assert_eq!(copy.zpop_min(Some(37)), popped);
        verify(&copy, &expected);
        verify(&set, &model);
    }
}

#[test]
fn invalid_increment_does_not_change_either_index() {
    let member = Bytes::from_static(b"infinite");
    let mut set = SortedSet::new();
    set.zadd(request(vec![(member.clone(), f64::INFINITY)]));
    assert_eq!(set.zincrby(member.clone(), f64::NEG_INFINITY), None);
    assert_eq!(set.zincrby(Bytes::from_static(b"missing"), f64::NAN), None);
    verify(&set, &HashMap::from([(member, f64::INFINITY)]));
}

#[test]
fn snapshot_rejects_duplicate_members_and_nan_scores() {
    let member = Bytes::from_static(b"member");
    for entries in [
        vec![(member.clone(), 1.0), (member.clone(), 2.0)],
        vec![(member, f64::NAN)],
    ] {
        let serialized = bincode2::serialize(&entries).unwrap();
        assert!(bincode2::deserialize::<SortedSet>(&serialized).is_err());
    }
    let empty = bincode2::serialize(&SortedSet::new()).unwrap();
    let restored: SortedSet = bincode2::deserialize(&empty).unwrap();
    verify(&restored, &HashMap::new());
}

#[test]
fn mixed_score_lex_ranges_preserve_existing_filter_behavior() {
    // Redis leaves BYLEX ordering unspecified when scores differ. Preserve
    // cache-cat's existing deterministic filtering for this case as well.
    let values = vec![
        (Bytes::from_static(b"z"), 0.0),
        (Bytes::from_static(b"b"), 1.0),
        (Bytes::from_static(b"a"), 2.0),
        (Bytes::from_static(b"c"), 3.0),
    ];
    let mut set = SortedSet::new();
    set.zadd(request(values));
    let lower = LexBound::Value {
        value: Bytes::from_static(b"a"),
        exclusive: false,
    };
    let upper = LexBound::Value {
        value: Bytes::from_static(b"c"),
        exclusive: false,
    };
    assert_eq!(
        set.zrange_lex(&lower, &upper, false, Some((1, -1))),
        vec![
            (Bytes::from_static(b"a"), 2.0),
            (Bytes::from_static(b"c"), 3.0)
        ]
    );
    assert_eq!(
        set.zrange_lex(&upper, &lower, true, Some((1, 1))),
        vec![(Bytes::from_static(b"a"), 2.0)]
    );
    assert!(
        set.zrangebyscore(
            f64::NEG_INFINITY,
            f64::INFINITY,
            Some((usize::MAX, usize::MAX))
        )
        .is_empty()
    );
    assert_eq!(
        set.zrangebyscore(f64::NEG_INFINITY, f64::INFINITY, Some((0, usize::MAX))),
        set.zrange(0, -1)
    );
}

#[test]
fn large_set_range_pages_survive_bulk_deletion_and_refill() {
    let mut set = SortedSet::new();
    let mut model: HashMap<Bytes, f64> = (0..8192)
        .map(|i| (Bytes::from(format!("entry:{i:05}")), (i % 131) as f64))
        .collect();
    set.zadd(request(
        model
            .iter()
            .map(|(member, score)| (member.clone(), *score))
            .collect(),
    ));
    let popped: Vec<_> = sorted(&model).into_iter().take(4100).collect();
    assert_eq!(set.zpop_min(Some(4100)), popped);
    for (member, _) in popped {
        model.remove(&member);
    }
    let removed: Vec<_> = model
        .iter()
        .filter(|(_, score)| **score as usize % 3 != 0)
        .map(|(member, _)| member.clone())
        .collect();
    assert_eq!(set.zrem(&removed), removed.len() as i64);
    for member in removed {
        model.remove(&member);
    }
    for i in 0..4096 {
        let member = Bytes::from(format!("refill:{i:05}"));
        let score = (i % 73) as f64 - 20.0;
        set.zadd(request(vec![(member.clone(), score)]));
        model.insert(member, score);
    }
    verify(&set, &model);
    let ordered = sorted(&model);
    for start in (0..ordered.len()).step_by(127) {
        let end = (start + 47).min(ordered.len());
        assert_eq!(
            set.zrange(start as i64, end as i64 - 1),
            ordered[start..end]
        );
        let reversed: Vec<_> = ordered.iter().rev().skip(start).take(47).cloned().collect();
        assert_eq!(
            set.zrange_rank(start as i64, start as i64 + 46, true),
            reversed
        );
    }
}
