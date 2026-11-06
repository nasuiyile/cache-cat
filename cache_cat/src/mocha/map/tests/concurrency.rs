//! Concurrent histories checked against the map's observable semantics.

use super::super::{Compute, HashMap, Operation};
use super::map;
use crossbeam_channel::{Receiver, bounded};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(15);

fn completions<T>(receiver: &Receiver<T>, count: usize) -> Vec<T> {
    (0..count)
        .map(|_| {
            receiver
                .recv_timeout(TIMEOUT)
                .expect("all concurrent operations must complete before joining threads")
        })
        .collect()
}

#[test]
fn atomic_increments_return_each_old_value_once_during_growth() {
    const WRITERS: usize = 4;
    const ROUNDS: usize = 8;
    const INCREMENTS: usize = 64;
    const NEW_KEYS: usize = 256;
    let map = Arc::new(map(1));
    for key in 0..2 {
        map.pin().insert(key, 0);
    }
    let initial_capacity = map.raw.test_root_capacity();
    let phases = Arc::new(Barrier::new(WRITERS + 1));
    let (done_tx, done_rx) = bounded(WRITERS + 1);
    let mut threads = Vec::new();
    for writer in 0..WRITERS {
        let map = map.clone();
        let phases = phases.clone();
        let done = done_tx.clone();
        threads.push(std::thread::spawn(move || {
            let mut observed = Vec::with_capacity(ROUNDS * INCREMENTS);
            for _ in 0..ROUNDS {
                phases.wait();
                for step in 0..INCREMENTS {
                    let key = ((writer + step) % 2) as u64;
                    let pin = map.pin();
                    match pin.compute(key, |current| {
                        Operation::<_, ()>::Insert(current.unwrap().1 + 1)
                    }) {
                        Compute::Updated {
                            old: (_, old),
                            new: (_, new),
                        } => {
                            assert_eq!(*new, *old + 1);
                            observed.push((key, *old));
                        }
                        _ => panic!("increment must replace its continuously present key"),
                    }
                }
                phases.wait();
            }
            done.send(observed).unwrap();
        }));
    }
    let growing = map.clone();
    threads.push(std::thread::spawn(move || {
        for round in 0..ROUNDS {
            phases.wait();
            for offset in 0..NEW_KEYS {
                let key = 2 + (round * NEW_KEYS + offset) as u64;
                assert!(growing.pin().insert(key, key).is_none());
            }
            phases.wait();
        }
        done_tx.send(Vec::new()).unwrap();
    }));
    // Never join before receiving bounded completion: a broken migration or
    // wakeup should fail this test instead of hanging the entire test process.
    let batches = completions(&done_rx, WRITERS + 1);
    for thread in threads {
        thread.join().unwrap();
    }
    let mut returned = [BTreeSet::new(), BTreeSet::new()];
    for (key, old) in batches.into_iter().flatten() {
        assert!(
            returned[key as usize].insert(old),
            "two increments returned the same old value"
        );
    }
    let expected = (WRITERS * ROUNDS * INCREMENTS / 2) as u64;
    for key in 0..2 {
        assert_eq!(map.pin().get(&key), Some(&expected));
        assert_eq!(returned[key as usize], (0..expected).collect());
    }
    assert_eq!(map.pin().len(), 2 + ROUNDS * NEW_KEYS);
    assert!(
        map.raw.test_root_capacity() > initial_capacity,
        "fixture must exercise real growth"
    );
}

#[test]
fn stale_expiration_rechecks_after_replacement_and_recreation_during_growth() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Expiring {
        generation: u64,
        deadline: u64,
    }
    for recreate in [false, true] {
        let map = Arc::new(HashMap::<u64, Expiring>::with_capacity(1));
        let old = Expiring {
            generation: 1,
            deadline: 100,
        };
        let new = Expiring {
            generation: 2,
            deadline: 200,
        };
        map.pin().insert(0, old);
        let initial_capacity = map.raw.test_root_capacity();
        let (observed_tx, observed_rx) = bounded(1);
        let (resume_tx, resume_rx) = bounded(1);
        let (done_tx, done_rx) = bounded(1);
        let expiring_map = map.clone();
        let expirer = std::thread::spawn(move || {
            let pin = expiring_map.pin();
            let mut paused = false;
            let result = pin.compute(0, |current| {
                let due = current.is_some_and(|(_, value)| value.deadline == 100);
                if due && !paused {
                    // Test-only scheduling: pause after examining the expired
                    // value, before publishing its conditional removal.
                    paused = true;
                    observed_tx.send(()).unwrap();
                    resume_rx.recv_timeout(TIMEOUT).unwrap();
                }
                if due {
                    Operation::Remove
                } else {
                    Operation::Abort(())
                }
            });
            let removed = match result {
                Compute::Removed(_, value) => Some(*value),
                Compute::Aborted(()) => None,
                _ => panic!("expiration must not insert a value"),
            };
            done_tx.send(removed).unwrap();
        });
        observed_rx.recv_timeout(TIMEOUT).unwrap();
        if recreate {
            assert_eq!(map.pin().remove(&0), Some(&old));
            assert!(map.pin().insert(0, new).is_none());
        } else {
            assert_eq!(map.pin().insert(0, new), Some(&old));
        }
        for key in 1..=2048 {
            map.pin().insert(
                key,
                Expiring {
                    generation: key,
                    deadline: 1_000,
                },
            );
        }
        assert!(map.raw.test_root_capacity() > initial_capacity);
        resume_tx.send(()).unwrap();
        assert_eq!(
            done_rx.recv_timeout(TIMEOUT).unwrap(),
            None,
            "stale expiry must not delete the newer entry"
        );
        expirer.join().unwrap();
        assert_eq!(map.pin().get(&0), Some(&new));
        assert_eq!(map.pin().len(), 2049);
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Get(usize),
    Insert(usize, i64),
    Remove(usize),
    Add(usize, i64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Response {
    Value(Option<i64>),
    Added { old: Option<i64>, new: i64 },
}

#[derive(Clone, Copy, Debug)]
struct Event {
    action: Action,
    response: Response,
    began: usize,
    ended: usize,
}

fn specification(action: Action, state: &mut [Option<i64>; 2]) -> Response {
    match action {
        Action::Get(key) => Response::Value(state[key]),
        Action::Insert(key, value) => Response::Value(state[key].replace(value)),
        Action::Remove(key) => Response::Value(state[key].take()),
        Action::Add(key, increment) => {
            let old = state[key];
            let new = old.unwrap_or(0) + increment;
            state[key] = Some(new);
            Response::Added { old, new }
        }
    }
}

fn linearizable(
    history: &[Event],
    initial: [Option<i64>; 2],
    final_state: [Option<i64>; 2],
) -> bool {
    assert!(history.len() < u16::BITS as usize);
    let predecessors: Vec<u16> = history
        .iter()
        .map(|event| {
            history
                .iter()
                .enumerate()
                .fold(0, |mask, (index, earlier)| {
                    if earlier.ended < event.began {
                        mask | (1 << index)
                    } else {
                        mask
                    }
                })
        })
        .collect();
    fn search(
        history: &[Event],
        predecessors: &[u16],
        state: [Option<i64>; 2],
        final_state: [Option<i64>; 2],
        remaining: u16,
        rejected: &mut HashSet<(u16, [Option<i64>; 2])>,
    ) -> bool {
        if remaining == 0 {
            return state == final_state;
        }
        if !rejected.insert((remaining, state)) {
            return false;
        }
        history.iter().enumerate().any(|(index, event)| {
            let bit = 1 << index;
            if remaining & bit == 0 || predecessors[index] & remaining != 0 {
                return false;
            }
            let mut next = state;
            specification(event.action, &mut next) == event.response
                && search(
                    history,
                    predecessors,
                    next,
                    final_state,
                    remaining & !bit,
                    rejected,
                )
        })
    }
    search(
        history,
        &predecessors,
        initial,
        final_state,
        (1 << history.len()) - 1,
        &mut HashSet::new(),
    )
}

#[test]
fn short_concurrent_histories_are_linearizable_with_real_time_order() {
    // Sanity-check the oracle: the same empty read is legal when overlapping
    // insertion, and illegal once that insertion has already returned.
    let mut oracle = [
        Event {
            action: Action::Insert(0, 7),
            response: Response::Value(None),
            began: 1,
            ended: 2,
        },
        Event {
            action: Action::Get(0),
            response: Response::Value(None),
            began: 0,
            ended: 3,
        },
    ];
    assert!(linearizable(&oracle, [None, None], [Some(7), None]));
    oracle[1].began = 3;
    oracle[1].ended = 4;
    assert!(!linearizable(&oracle, [None, None], [Some(7), None]));
    const ROUNDS: usize = 32;
    const WORKERS: usize = 3;
    let map = Arc::new(HashMap::<u64, i64>::with_capacity(1));
    let phases = Arc::new(Barrier::new(WORKERS + 2));
    let sequence = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = bounded(WORKERS + 1);
    let mut threads = Vec::new();
    for worker in 0..WORKERS {
        let map = map.clone();
        let phases = phases.clone();
        let sequence = sequence.clone();
        let done = done_tx.clone();
        threads.push(std::thread::spawn(move || {
            for round in 0..ROUNDS {
                let first = round % 2;
                let second = first ^ 1;
                let token = 1_000 + round as i64 * 10;
                let actions = match worker {
                    0 => [
                        Action::Insert(first, token),
                        Action::Get(second),
                        Action::Add(first, 1),
                    ],
                    1 => [
                        Action::Get(first),
                        Action::Insert(second, token + 2),
                        Action::Remove(first),
                    ],
                    _ => [
                        Action::Add(second, 3),
                        Action::Remove(second),
                        Action::Get(first),
                    ],
                };
                phases.wait();
                let mut history = Vec::with_capacity(actions.len());
                for action in actions {
                    let began = sequence.fetch_add(1, Ordering::SeqCst);
                    let response = {
                        let pin = map.pin();
                        match action {
                            Action::Get(key) => Response::Value(pin.get(&(key as u64)).copied()),
                            Action::Insert(key, value) => {
                                Response::Value(pin.insert(key as u64, value).copied())
                            }
                            Action::Remove(key) => {
                                Response::Value(pin.remove(&(key as u64)).copied())
                            }
                            Action::Add(key, increment) => match pin.compute(key as u64, |entry| {
                                Operation::<_, ()>::Insert(
                                    entry.map_or(0, |(_, value)| *value) + increment,
                                )
                            }) {
                                Compute::Inserted(_, value) => Response::Added {
                                    old: None,
                                    new: *value,
                                },
                                Compute::Updated {
                                    old: (_, old),
                                    new: (_, new),
                                } => Response::Added {
                                    old: Some(*old),
                                    new: *new,
                                },
                                _ => panic!("addition must install its result"),
                            },
                        }
                    };
                    let ended = sequence.fetch_add(1, Ordering::SeqCst);
                    history.push(Event {
                        action,
                        response,
                        began,
                        ended,
                    });
                }
                done.send(history).unwrap();
            }
        }));
    }
    let growing_map = map.clone();
    let growing_phases = phases.clone();
    threads.push(std::thread::spawn(move || {
        for round in 0..ROUNDS {
            growing_phases.wait();
            // Unrecorded independent keys exercise growth and tombstone
            // cleanup; they cannot alter the two-key reference state.
            for key in 2..(66 + (round % 3) * 96) as u64 {
                growing_map.pin().insert(key, key as i64);
            }
            done_tx.send(Vec::new()).unwrap();
        }
    }));
    for round in 0..ROUNDS {
        map.pin().clear();
        let initial = [Some(round as i64), None];
        map.pin().insert(0, initial[0].unwrap());
        phases.wait();
        let mut history: Vec<_> = completions(&done_rx, WORKERS + 1)
            .into_iter()
            .flatten()
            .collect();
        history.sort_unstable_by_key(|event| event.began);
        assert_eq!(history.len(), 9);
        let pin = map.pin();
        let final_state = [pin.get(&0).copied(), pin.get(&1).copied()];
        assert!(
            linearizable(&history, initial, final_state),
            "round {round}: no legal real-time-respecting sequential history: {history:?}; final={final_state:?}"
        );
    }
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn disjoint_key_lifecycles_preserve_exact_final_state_under_concurrent_growth() {
    const WORKERS: usize = 4;
    const KEYS_PER_WORKER: usize = 128;
    let map = Arc::new(map(1));
    let start = Arc::new(Barrier::new(WORKERS));
    let (done_tx, done_rx) = bounded(WORKERS);
    let mut threads = Vec::new();
    for worker in 0..WORKERS {
        let map = map.clone();
        let start = start.clone();
        let done = done_tx.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            for index in 0..KEYS_PER_WORKER {
                let key = (index * WORKERS + worker) as u64;
                let pin = map.pin();
                assert!(pin.insert(key, key).is_none());
                match pin.compute(key, |entry| {
                    Operation::<_, ()>::Insert(entry.unwrap().1 + 10)
                }) {
                    Compute::Updated {
                        old: (_, old),
                        new: (_, new),
                    } => {
                        assert_eq!(*old, key);
                        assert_eq!(*new, key + 10);
                    }
                    _ => panic!("owned key must still be present for its update"),
                }
                if index % 3 == 0 {
                    assert_eq!(pin.remove(&key), Some(&(key + 10)));
                }
                if index % 5 == 0 {
                    let previous = if index % 3 == 0 { None } else { Some(key + 10) };
                    assert_eq!(pin.insert(key, key + 100).copied(), previous);
                }
            }
            done.send(()).unwrap();
        }));
    }
    drop(done_tx);
    completions(&done_rx, WORKERS);
    for thread in threads {
        thread.join().unwrap();
    }
    let expected: BTreeMap<_, _> = (0..WORKERS * KEYS_PER_WORKER)
        .filter_map(|key| {
            let index = key / WORKERS;
            let value = if index % 5 == 0 {
                Some(key as u64 + 100)
            } else if index % 3 == 0 {
                None
            } else {
                Some(key as u64 + 10)
            };
            value.map(|value| (key as u64, value))
        })
        .collect();
    let pin = map.pin();
    let entries: Vec<_> = pin.iter().map(|(key, value)| (*key, *value)).collect();
    assert_eq!(entries.len(), expected.len());
    assert_eq!(pin.len(), expected.len());
    assert_eq!(entries.into_iter().collect::<BTreeMap<_, _>>(), expected);
}
