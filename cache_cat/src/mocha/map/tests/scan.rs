use super::{TestMap, map};
use std::collections::{BTreeMap, BTreeSet};

fn page(map: &TestMap, cursor: u64, count: usize, seen: &mut BTreeSet<u64>) -> u64 {
    map.pin().scan(cursor, count, |key, value| {
        // No writes occur inside this callback. A copied source must resolve
        // the currently visible value, rather than its obsolete payload.
        assert_eq!(map.pin().get(key), Some(value));
        seen.insert(*key);
    })
}

#[test]
fn every_page_boundary_survives_partial_growth_shrink_and_cleanup() {
    let keys: BTreeSet<u64> = (0..20).map(|key| key * 37 % 64).collect();
    let reference = map(32);
    for key in &keys {
        reference.pin().insert(*key, *key);
    }
    let mut boundaries = vec![0];
    let mut cursor = 0;
    loop {
        cursor = reference.pin().scan(cursor, 1, |_, _| {});
        if cursor == 0 {
            break;
        }
        boundaries.push(cursor);
    }
    assert!(
        boundaries.len() >= 10,
        "fixture must exercise many boundaries"
    );

    for (cut, expected_cursor) in boundaries.iter().copied().enumerate() {
        for capacity in [32, 64, 128] {
            for copied in [0, 1, 7, 31] {
                let map = map(32);
                for key in &keys {
                    map.pin().insert(*key, *key);
                }
                let mut seen = BTreeSet::new();
                let mut cursor = 0;
                for _ in 0..cut {
                    cursor = page(&map, cursor, 1, &mut seen);
                }
                assert_eq!(cursor, expected_cursor);
                map.raw.test_resize(capacity, copied);
                assert!(map.raw.test_has_pending_resize());

                for turn in 0..256 {
                    // Replacement must not change logical bucket coverage.
                    let key = *keys.iter().nth(turn % keys.len()).unwrap();
                    map.pin().insert(key, turn as u64 + 1000);
                    cursor = page(&map, cursor, [1, 2, 7][turn % 3], &mut seen);
                    if cursor == 0 {
                        break;
                    }
                    if turn == 1 {
                        map.raw.test_finish_resize();
                    }
                }
                assert_eq!(cursor, 0, "cut={cut}, capacity={capacity}, copied={copied}");
                assert_eq!(
                    seen, keys,
                    "cut={cut}, capacity={capacity}, copied={copied}"
                );
            }
        }
    }
}

#[test]
fn nested_resize_successors_preserve_current_values_and_new_keys() {
    // Copying a sparse large source into a small destination overflows the
    // destination's probe limit and creates a second successor. The original
    // source remains unfinished. This exercises a real insert_copy cascade,
    // not fabricated raw pointers or forced COPIED bits.
    let map = map(32768);
    let mut expected: BTreeMap<u64, u64> = (0..40)
        .map(|key| key * 65536)
        .chain(1..=24)
        .map(|key| (key, key))
        .collect();
    for (key, value) in &expected {
        map.pin().insert(*key, *value);
    }
    assert_eq!(map.raw.test_root_capacity(), 65536);
    map.raw.test_resize(128, 4096);
    assert!(map.raw.test_table_capacities().len() >= 3);

    let newcomer = 100 * 65536;
    map.pin().insert(newcomer, newcomer);
    expected.insert(newcomer, newcomer);
    assert!(!map.raw.test_root_contains(&newcomer));
    map.pin().insert(0, 9000);
    expected.insert(0, 9000);
    map.pin().remove(&65536);
    expected.remove(&65536);
    assert!(
        map.raw.test_root_contains(&65536),
        "source must retain a stale copy"
    );

    let capacities = map.raw.test_table_capacities();
    for count in [1, 17, usize::MAX] {
        let mut seen = BTreeMap::new();
        let mut cursor = 0;
        for _ in 0..100_000 {
            cursor = map.pin().scan(cursor, count, |key, value| {
                assert_eq!(expected.get(key), Some(value));
                seen.insert(*key, *value);
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(cursor, 0, "count={count}");
        assert_eq!(seen, expected, "count={count}");
        assert_eq!(map.raw.test_table_capacities(), capacities);
    }
    assert_eq!(map.pin().len(), expected.len());
}

#[test]
fn removing_each_visited_entry_keeps_callback_references_valid() {
    let map = map(256);
    for key in 0..128 {
        map.pin().insert(key, key + 1000);
    }
    let mut seen = BTreeSet::new();
    let mut cursor = 0;
    for _ in 0..1024 {
        cursor = map.pin().scan(cursor, 3, |key, value| {
            let pin = map.pin();
            assert_eq!(pin.remove(key), Some(value));
            assert_eq!(*value, *key + 1000);
            assert!(seen.insert(*key));
            assert_eq!(pin.get(key), None);
        });
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(cursor, 0);
    assert_eq!(seen, (0..128).collect());
    assert_eq!(map.pin().len(), 0);
    assert_eq!(map.pin().scan(0, 1, |_, _| panic!("empty")), 0);
}

#[test]
fn independent_cursors_survive_interleaved_resize_and_value_updates() {
    let map = map(128);
    let expected: BTreeSet<u64> = (0..64).collect();
    for key in &expected {
        map.pin().insert(*key, *key);
    }
    let mut cursors = [0, 0];
    let mut done = [false, false];
    let mut seen = [BTreeSet::new(), BTreeSet::new()];
    for turn in 0..1024 {
        for reader in 0..2 {
            if !done[reader] {
                cursors[reader] = page(
                    &map,
                    cursors[reader],
                    if reader == 0 { 1 } else { 7 },
                    &mut seen[reader],
                );
                done[reader] = cursors[reader] == 0;
            }
        }
        if done == [true, true] {
            break;
        }
        match turn {
            0 => map.raw.test_resize(1024, 3),
            3 => map.raw.test_finish_resize(),
            5 => map.raw.test_resize(128, 5),
            8 => map.raw.test_finish_resize(),
            _ => {}
        }
        let key = turn as u64 % 64;
        map.pin().insert(key, turn as u64 + 5000);
    }
    assert_eq!(done, [true, true]);
    assert_eq!(seen[0], expected);
    assert_eq!(seen[1], expected);
}
