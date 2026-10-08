/// SplitMix64 with a fixed algorithm for commands replayed by every Raft node.
/// Using u64 state and u128 index mapping also avoids platform-sized sampling.
#[derive(Debug, Clone)]
pub(super) struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    pub(super) fn for_key(key: &[u8], logical_clock: u64) -> Self {
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
    pub(super) fn next_index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        let random = self.next_u64() as u128;
        let bound = upper_bound as u128;
        ((random * bound) >> 64) as usize
    }
}
