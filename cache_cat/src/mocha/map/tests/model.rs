//! Reproducible state-machine comparisons against an independent standard map.
use super::super::{Compute, Equivalent, HashMap, Operation};
use std::collections::{HashMap as StdMap, VecDeque};
use std::hash::{BuildHasher, DefaultHasher, Hash, Hasher};

#[derive(Clone, Copy)]
struct SeededHasher(u64);
impl BuildHasher for SeededHasher {
    type Hasher = DefaultHasher;
    fn build_hasher(&self) -> Self::Hasher {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(self.0);
        hasher
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key<const COLLISIONS: bool>(u64);
impl<const COLLISIONS: bool> Hash for Key<COLLISIONS> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Four keys share a full hash. An all-equal hasher would instead
        // require exponential growth under the map's probe-length bound.
        (if COLLISIONS { self.0 / 4 } else { self.0 }).hash(state);
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = self.0;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^ (word >> 31)
    }
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Insert(u64, i64),
    Get(u64),
    Remove(u64),
    ComputeInsert(u64, i64),
    ComputeRemove(u64, u64),
    ComputeAbort(u64, u64),
    Retain(u64),
    Clear,
    Iter,
    Scan,
}

#[derive(Debug)]
struct Context {
    seed: u64,
    collisions: bool,
    step: usize,
    recent: VecDeque<(usize, Action)>,
}
impl Context {
    fn record(&mut self, step: usize, action: Action) {
        self.step = step;
        if self.recent.len() == 24 {
            self.recent.pop_front();
        }
        self.recent.push_back((step, action));
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Inserted(u64, i64),
    Updated { old: (u64, i64), new: (u64, i64) },
    Removed(u64, i64),
    Aborted(u64),
}
fn outcome<const COLLISIONS: bool>(result: Compute<'_, Key<COLLISIONS>, i64, u64>) -> Outcome {
    match result {
        Compute::Inserted(key, value) => Outcome::Inserted(key.0, *value),
        Compute::Updated { old, new } => Outcome::Updated {
            old: (old.0.0, *old.1),
            new: (new.0.0, *new.1),
        },
        Compute::Removed(key, value) => Outcome::Removed(key.0, *value),
        Compute::Aborted(token) => Outcome::Aborted(token),
    }
}
type ModelMap<const COLLISIONS: bool> = HashMap<Key<COLLISIONS>, i64, SeededHasher>;

fn assert_iteration<const COLLISIONS: bool>(
    actual: &ModelMap<COLLISIONS>,
    expected: &StdMap<u64, i64>,
    context: &Context,
) {
    let pin = actual.pin();
    let mut found = StdMap::new();
    for (key, value) in pin.iter() {
        assert_eq!(
            found.insert(key.0, *value),
            None,
            "duplicate iter key: {context:#?}"
        );
    }
    assert_eq!(&found, expected, "iteration mismatch: {context:#?}");
    assert_eq!(pin.len(), expected.len(), "length mismatch: {context:#?}");
}

fn assert_scan<const COLLISIONS: bool>(
    actual: &ModelMap<COLLISIONS>,
    expected: &StdMap<u64, i64>,
    context: &Context,
) {
    let counts = [1, 13, 2, 29, 3, 7];
    let mut cursor = 0;
    let mut found = StdMap::new();
    for page in 0..16_384 {
        cursor = actual.pin().scan(
            cursor,
            counts[(page + context.step) % counts.len()],
            |key, value| {
                assert_eq!(
                    expected.get(&key.0),
                    Some(value),
                    "unknown/stale SCAN entry: {context:#?}"
                );
                // A pending migration permits duplicates even without further writes.
                found.insert(key.0, *value);
            },
        );
        if cursor == 0 {
            assert_eq!(&found, expected, "SCAN coverage mismatch: {context:#?}");
            return;
        }
    }
    panic!("SCAN did not terminate; cursor={cursor}: {context:#?}");
}
fn keep(key: u64, value: i64, salt: u64) -> bool {
    (key.wrapping_add(value as u64).rotate_left(7) ^ salt) % 5 != 0
}

fn run_model<const COLLISIONS: bool>(seed: u64) {
    let actual = ModelMap::<COLLISIONS>::with_hasher(SeededHasher(seed));
    let mut expected = StdMap::new();
    let mut random = Rng(seed);
    let mut context = Context {
        seed,
        collisions: COLLISIONS,
        step: 0,
        recent: VecDeque::new(),
    };
    let mut exercised = [0usize; 10];

    // Start with actual growth. SCAN precedes physical iteration, which could
    // otherwise complete migration before scan coverage gets checked.
    for key in 0..384 {
        let value = random.next() as i64;
        context.record(key as usize, Action::Insert(key, value));
        assert_eq!(
            actual.pin().insert(Key(key), value).copied(),
            expected.insert(key, value),
            "{context:#?}"
        );
    }
    assert_scan(&actual, &expected, &context);
    assert_iteration(&actual, &expected, &context);

    for step in 0..4096 {
        let key = random.next() % 512;
        let operand = random.next();
        let selector = random.next() % 100;
        let action = if step % 1024 == 1023 {
            Action::Clear
        } else {
            match selector {
                0..=31 => Action::Insert(key, operand as i64),
                32..=45 => Action::Get(key),
                46..=60 => Action::Remove(key),
                61..=77 => Action::ComputeInsert(key, operand as i64),
                78..=85 => Action::ComputeRemove(key, operand),
                86..=92 => Action::ComputeAbort(key, operand),
                93..=95 => Action::Retain(operand),
                96..=97 => Action::Iter,
                _ => Action::Scan,
            }
        };
        context.record(step + 384, action);
        let pin = actual.pin();
        match action {
            Action::Insert(key, value) => {
                exercised[0] += 1;
                assert_eq!(
                    pin.insert(Key(key), value).copied(),
                    expected.insert(key, value),
                    "{context:#?}"
                );
            }
            Action::Get(key) => {
                exercised[1] += 1;
                assert_eq!(pin.get(&Key(key)), expected.get(&key), "{context:#?}");
                assert_eq!(
                    pin.get_key_value(&Key(key))
                        .map(|(key, value)| (key.0, *value)),
                    expected.get(&key).map(|value| (key, *value)),
                    "{context:#?}"
                );
            }
            Action::Remove(key) => {
                exercised[2] += 1;
                assert_eq!(
                    pin.remove(&Key(key)).copied(),
                    expected.remove(&key),
                    "{context:#?}"
                );
            }
            Action::ComputeInsert(key, delta) => {
                exercised[3] += 1;
                let previous = expected.get(&key).copied();
                let next = previous.unwrap_or(0).wrapping_add(delta);
                let result = pin.compute(Key(key), |current| {
                    assert_eq!(
                        current.map(|(stored, value)| (stored.0, *value)),
                        previous.map(|value| (key, value)),
                        "compute input: {context:#?}"
                    );
                    Operation::Insert(current.map_or(0, |(_, value)| *value).wrapping_add(delta))
                });
                let wanted = match previous {
                    None => Outcome::Inserted(key, next),
                    Some(old) => Outcome::Updated {
                        old: (key, old),
                        new: (key, next),
                    },
                };
                assert_eq!(outcome(result), wanted, "compute result: {context:#?}");
                expected.insert(key, next);
            }
            Action::ComputeRemove(key, token) => {
                exercised[4] += 1;
                let previous = expected.remove(&key);
                let result = pin.compute(Key(key), |current| {
                    assert_eq!(
                        current.map(|(stored, value)| (stored.0, *value)),
                        previous.map(|value| (key, value)),
                        "compute input: {context:#?}"
                    );
                    if current.is_some() {
                        Operation::Remove
                    } else {
                        Operation::Abort(token)
                    }
                });
                assert_eq!(
                    outcome(result),
                    previous.map_or(Outcome::Aborted(token), |value| Outcome::Removed(
                        key, value
                    )),
                    "compute result: {context:#?}"
                );
            }
            Action::ComputeAbort(key, token) => {
                exercised[5] += 1;
                let previous = expected.get(&key).copied();
                let result = pin.compute(Key(key), |current| {
                    assert_eq!(
                        current.map(|(stored, value)| (stored.0, *value)),
                        previous.map(|value| (key, value)),
                        "compute input: {context:#?}"
                    );
                    Operation::Abort(token)
                });
                assert_eq!(outcome(result), Outcome::Aborted(token), "{context:#?}");
                assert_eq!(
                    pin.get(&Key(key)).copied(),
                    previous,
                    "abort modified state: {context:#?}"
                );
            }
            Action::Retain(salt) => {
                exercised[6] += 1;
                pin.retain(|key, value| keep(key.0, *value, salt));
                expected.retain(|key, value| keep(*key, *value, salt));
            }
            Action::Clear => {
                exercised[7] += 1;
                pin.clear();
                expected.clear();
            }
            Action::Iter => {
                exercised[8] += 1;
                assert_iteration(&actual, &expected, &context);
            }
            Action::Scan => {
                exercised[9] += 1;
                assert_scan(&actual, &expected, &context);
            }
        }
        assert_eq!(
            pin.len(),
            expected.len(),
            "post-operation length: {context:#?}"
        );
        drop(pin);
        if step % 97 == 0 {
            assert_scan(&actual, &expected, &context);
            assert_iteration(&actual, &expected, &context);
        }
    }
    assert!(
        exercised.iter().all(|count| *count > 0),
        "missing operation: {exercised:?}; {context:#?}"
    );
    assert_scan(&actual, &expected, &context);
    assert_iteration(&actual, &expected, &context);
}

#[test]
fn randomized_operations_match_standard_map() {
    for seed in [0, 1, 0x1234_5678_9abc_def0, 0xdead_beef_cafe_f00d, u64::MAX] {
        run_model::<false>(seed);
    }
}
#[test]
fn randomized_operations_with_hash_collisions_match_standard_map() {
    for seed in [0, 1, 0x1234_5678_9abc_def0, 0xdead_beef_cafe_f00d, u64::MAX] {
        run_model::<true>(seed);
    }
}

#[test]
fn borrowed_string_and_binary_keys_match_standard_map() {
    let strings = ["", "ordinary", "embedded\0nul", "中文:键", "é/🦀"];
    let actual = HashMap::with_hasher(SeededHasher(41));
    let mut expected = StdMap::new();
    for (value, key) in strings.into_iter().enumerate() {
        actual.pin().insert(key.to_owned(), value);
        expected.insert(key.to_owned(), value);
    }
    for key in strings {
        let pin = actual.pin();
        assert_eq!(pin.get(key), expected.get(key));
        assert_eq!(pin.get_key_value(key), expected.get_key_value(key));
        assert_eq!(pin.remove(key).copied(), expected.remove(key));
        assert_eq!(pin.get(key), None);
    }

    let binary = HashMap::with_hasher(SeededHasher(99));
    let mut expected = StdMap::new();
    let mut keys = vec![vec![], vec![0], vec![0xff, 0x80], b"a\0b".to_vec()];
    keys.extend((0..256u16).map(|number| [number.to_le_bytes().as_slice(), &[0, 0xff]].concat()));
    for (value, key) in keys.iter().enumerate() {
        binary.pin().insert(key.clone(), value);
        expected.insert(key.clone(), value);
    }
    for (index, key) in keys.iter().enumerate() {
        let query = key.as_slice();
        let pin = binary.pin();
        assert_eq!(pin.get(query), expected.get(query), "binary key {key:?}");
        assert_eq!(pin.get_key_value(query), expected.get_key_value(query));
        if index % 2 == 0 {
            assert_eq!(pin.remove(query).copied(), expected.remove(query));
        } else {
            assert_eq!(
                pin.insert(key.clone(), index + 1000).copied(),
                expected.insert(key.clone(), index + 1000)
            );
        }
    }
    let pin = binary.pin();
    assert_eq!(pin.len(), expected.len());
    assert_eq!(
        pin.iter()
            .map(|(key, value)| (key.clone(), *value))
            .collect::<StdMap<_, _>>(),
        expected
    );
}

#[derive(Debug)]
struct IdentityKey {
    id: u64,
    identity: u32,
}
impl PartialEq for IdentityKey {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for IdentityKey {}
impl Hash for IdentityKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
struct Query(u64);
impl Hash for Query {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl Equivalent<IdentityKey> for Query {
    fn equivalent(&self, key: &IdentityKey) -> bool {
        self.0 == key.id
    }
}

#[test]
fn equivalent_queries_and_stored_key_identity_preserve_papaya_behavior() {
    // A standard map keeps the original key object. For non-identical equal
    // keys, the legacy concurrent container is the compatibility oracle.
    let actual = HashMap::with_hasher(SeededHasher(7));
    let original = papaya::HashMap::with_hasher(SeededHasher(7));
    let pin = actual.pin();
    let original = original.pin();
    for identity in 0..4 {
        assert_eq!(
            pin.insert(IdentityKey { id: 7, identity }, identity),
            original.insert(IdentityKey { id: 7, identity }, identity)
        );
        let (key, value) = pin.get_key_value(&Query(7)).unwrap();
        let (old_key, old_value) = original.get_key_value(&Query(7)).unwrap();
        assert_eq!(
            (key.id, key.identity, *value),
            (old_key.id, old_key.identity, *old_value)
        );
    }
    assert_eq!(pin.get(&Query(8)), None);
    assert_eq!(
        pin.compute(
            IdentityKey {
                id: 7,
                identity: 99
            },
            |_| Operation::<_, u8>::Abort(42)
        ),
        Compute::Aborted(42)
    );
    assert_eq!(
        pin.get_key_value(&Query(7)).unwrap().0.identity,
        original.get_key_value(&Query(7)).unwrap().0.identity
    );
    assert_eq!(pin.remove(&Query(7)), original.remove(&Query(7)));
    assert_eq!(pin.len(), 0);
}
