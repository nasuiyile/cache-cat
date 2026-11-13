use std::collections::{HashMap, TryReserveError};

/// SplitMix64 with a fixed algorithm for commands replayed by every Raft node.
/// Using u64 state and u128 index mapping also avoids platform-sized sampling.
#[derive(Debug, Clone)]
pub(crate) struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub(crate) fn for_key(key: &[u8], logical_clock: u64) -> Self {
        // Preserve SPOP's existing FNV-1a seed and clock mixing.
        let mut hash = 0xCBF2_9CE4_8422_2325u64;
        for byte in key {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
        Self {
            state: hash ^ logical_clock.rotate_left(17) ^ 0xA076_1D64_78BD_642F,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    /// Return an index in [0, upper_bound), independent of pointer width.
    pub(crate) fn next_index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        let random = self.next_u64() as u128;
        let bound = upper_bound as u128;
        ((random * bound) >> 64) as usize
    }
}

/// Visit distinct dense indices without modifying the sampled collection.
/// Sampling a small subset uses a virtual partial Fisher-Yates shuffle; its
/// remapping table is only queried by index, so local hash seeds cannot affect
/// the selected sequence. Larger subsets use a compact index vector instead.
/// Both paths take expected O(count) time and O(count) scratch space, and never
/// copy members or values. Returning the whole collection preserves dense order.
pub(crate) fn sample_unique_indices<R, V>(
    len: usize,
    count: usize,
    mut next_index: R,
    mut visit: V,
) -> Result<(), TryReserveError>
where
    R: FnMut(usize) -> usize,
    V: FnMut(usize),
{
    debug_assert!(count <= len);
    if count == len {
        for index in 0..len {
            visit(index);
        }
    } else if count == 1 {
        visit(next_index(len));
    } else if count > len / 4 {
        // Here len < 4 * count, so scratch space remains O(count).
        // Compact indices halve scratch storage on 64-bit targets for common sizes.
        // Their representation cannot affect the RNG bounds or selected order.
        if let Ok(len32) = u32::try_from(len) {
            let mut indices = Vec::new();
            indices.try_reserve_exact(len)?;
            indices.extend(0..len32);
            sample_dense_indices(indices, count, next_index, |index| visit(index as usize));
        } else {
            let mut indices = Vec::new();
            indices.try_reserve_exact(len)?;
            indices.extend(0..len);
            sample_dense_indices(indices, count, next_index, visit);
        }
    } else if count != 0 {
        let mut moved = HashMap::with_hasher(ahash::RandomState::new());
        moved.try_reserve(count)?;
        for remaining in ((len - count + 1)..=len).rev() {
            let index = next_index(remaining);
            let selected = moved.remove(&index).unwrap_or(index);
            let last = remaining - 1;
            let replacement = moved.remove(&last).unwrap_or(last);
            if index != last {
                moved.insert(index, replacement);
            }
            visit(selected);
        }
    }
    Ok(())
}

fn sample_dense_indices<I: Copy>(
    mut indices: Vec<I>,
    count: usize,
    mut next_index: impl FnMut(usize) -> usize,
    mut visit: impl FnMut(I),
) {
    let len = indices.len();
    for remaining in ((len - count + 1)..=len).rev() {
        let index = next_index(remaining);
        indices.swap(index, remaining - 1);
    }
    // Emit after sampling so random scratch access does not interleave with
    // cloning values. Reversing the tail preserves swap_remove's selection order.
    for &index in indices[len - count..].iter().rev() {
        visit(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn indexed_samples_match_partial_shuffle_at_sparse_and_dense_sizes() {
        for len in [1, 2, 4, 17, 64, 1024] {
            for count in [0, 1, len / 4, len / 4 + 1, len - 1, len] {
                for clock in 0..8 {
                    let mut expected = Vec::new();
                    if count == len {
                        expected.extend(0..len);
                    } else {
                        let mut candidates: Vec<_> = (0..len).collect();
                        let mut rng = DeterministicRng::for_key(b"sample", clock);
                        for _ in 0..count {
                            let index = rng.next_index(candidates.len());
                            expected.push(candidates.swap_remove(index));
                        }
                    }
                    let mut actual = Vec::new();
                    let mut rng = DeterministicRng::for_key(b"sample", clock);
                    sample_unique_indices(
                        len,
                        count,
                        |bound| rng.next_index(bound),
                        |index| actual.push(index),
                    )
                    .unwrap();
                    assert_eq!(actual, expected, "len={len}, count={count}, clock={clock}");
                    assert_eq!(actual.iter().collect::<HashSet<_>>().len(), count);
                }
            }
        }
    }

    #[test]
    fn zero_and_full_samples_need_no_random_draws() {
        for (len, count) in [(0, 0), (16, 0), (16, 16)] {
            let mut indices = Vec::new();
            sample_unique_indices(
                len,
                count,
                |_| panic!("no random draw needed"),
                |index| indices.push(index),
            )
            .unwrap();
            assert_eq!(indices, (0..count).collect::<Vec<_>>());
        }
    }

    #[test]
    fn tiny_sample_does_not_allocate_in_proportion_to_population() {
        let mut indices = Vec::new();
        sample_unique_indices(usize::MAX, 3, |_| 0, |index| indices.push(index)).unwrap();
        assert_eq!(indices, [0, usize::MAX - 1, usize::MAX - 2]);
    }

    #[test]
    fn dense_index_width_does_not_change_the_selected_sequence() {
        for clock in 0..8 {
            let mut compact_rng = DeterministicRng::for_key(b"sample", clock);
            let mut native_rng = DeterministicRng::for_key(b"sample", clock);
            let mut compact = Vec::new();
            let mut native = Vec::new();
            sample_dense_indices(
                (0..128_u32).collect(),
                96,
                |bound| compact_rng.next_index(bound),
                |index| compact.push(index as usize),
            );
            sample_dense_indices(
                (0..128_usize).collect(),
                96,
                |bound| native_rng.next_index(bound),
                |index| native.push(index),
            );
            assert_eq!(compact, native);
        }
    }
}
