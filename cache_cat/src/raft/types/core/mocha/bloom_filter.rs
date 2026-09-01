use serde::{Deserialize, Serialize};
use std::mem::size_of;

/// Redis 默认 Bloom Filter 配置。
///
/// Redis 8 中这些值实际上可以通过配置修改：
///
/// bf-error-rate       = 0.01
/// bf-initial-size     = 100
/// bf-expansion-factor = 2
pub const DEFAULT_BLOOM_ERROR_RATE: f64 = 0.01;
pub const DEFAULT_BLOOM_CAPACITY: u64 = 100;
pub const DEFAULT_BLOOM_EXPANSION: u32 = 2;

/// RedisBloom scalable bloom filter 每新增一层，
/// error rate 都乘这个比例。
const ERROR_TIGHTENING_RATIO: f64 = 0.5;

/// ln(2)^2
const BPE_DENOM: f64 = 0.480453013918201;

/// ln(2)
const LN2: f64 = std::f64::consts::LN_2;

/// RedisBloom FORCE64 使用的 MurmurHash64A seed。
const BLOOM_HASH_SEED: u64 = 0xc6a4a7935bd1e995;


/// Redis accepts an error rate < 1, but internally caps values > 0.25.
pub const BLOOM_ERROR_RATE_CAP: f64 = 0.25;

pub const BLOOM_CAPACITY_MIN: u64 = 1;
pub const BLOOM_CAPACITY_MAX: u64 = 1_048_576;

pub const BLOOM_EXPANSION_MIN: u32 = 0;
pub const BLOOM_EXPANSION_MAX: u32 = 32_768;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomError {
    /// NONSCALING filter 满了。
    Full,

    /// 内存分配失败。
    OutOfMemory,

    /// 参数或者内部状态非法。
    Invalid,

    /// 容量计算发生溢出。
    Overflow,
}

/// 一个 Redis scalable Bloom Filter。
///
/// 它不是一个 bitmap，而是一组不断扩展的 sub-filter：
///
/// filter[0]
/// filter[1]
/// filter[2]
/// ...
///
/// BF.ADD 会先从最后一个 filter 往前检查是否存在，
/// 如果所有 filter 都判断不存在，才向最新 filter 插入。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BloomObject {
    filters: Vec<BloomSubFilter>,

    /// 整个 Bloom chain 中成功插入的数量。
    ///
    /// 注意：
    /// 这是 Bloom filter 认为“之前不存在”并成功加入的数量，
    /// 不是实际唯一元素的精确 cardinality。
    size: u64,

    /// 扩容倍数。
    ///
    /// 默认 2。
    growth: u32,

    /// 是否禁止扩容。
    non_scaling: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BloomSubFilter {
    /// Bitmap。
    bitmap: Vec<u8>,

    /// 这一层的逻辑 capacity。
    entries: u64,

    /// 这一层已经成功插入的元素数。
    size: u64,

    /// 当前 sub-filter 自己的 false positive rate。
    error: f64,

    /// bits per element。
    bpe: f64,

    /// hash probe 数量。
    hashes: u32,

    /// 实际 bitmap bit 数。
    ///
    /// RedisBloom 使用 FORCE64，因此会向 64 bit 对齐。
    bits: u64,
}

#[derive(Clone, Copy, Debug)]
struct BloomHash {
    a: u64,
    b: u64,
}

impl BloomObject {
    /// BF.ADD 在 key 不存在时使用的 Redis 默认配置。
    pub fn redis_default() -> Result<Self, BloomError> {
        Self::new(
            DEFAULT_BLOOM_CAPACITY,
            DEFAULT_BLOOM_ERROR_RATE,
            DEFAULT_BLOOM_EXPANSION,
            false,
        )
    }

    pub fn new(
        capacity: u64,
        error_rate: f64,
        expansion: u32,
        non_scaling: bool,
    ) -> Result<Self, BloomError> {
        if capacity == 0 || !error_rate.is_finite() || error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(BloomError::Invalid);
        }

        // Redis 的 expansion=0 等价于 NONSCALING。
        let non_scaling = non_scaling || expansion == 0;

        // Redis scalable Bloom Filter 第一层就会把 error rate
        // 收紧为请求值的 0.5。
        //
        // NONSCALING 不需要 tightening。
        let first_error = if non_scaling {
            error_rate
        } else {
            error_rate * ERROR_TIGHTENING_RATIO
        };

        if first_error <= 0.0 || !first_error.is_finite() {
            return Err(BloomError::Invalid);
        }

        let first = BloomSubFilter::new(capacity, first_error)?;

        let mut filters = Vec::new();

        filters
            .try_reserve_exact(1)
            .map_err(|_| BloomError::OutOfMemory)?;

        filters.push(first);

        Ok(Self {
            filters,
            size: 0,
            growth: expansion,
            non_scaling,
        })
    }

    /// Redis BF.ADD 的核心逻辑。
    ///
    /// 返回：
    ///
    /// true  => item 被成功加入
    /// false => Bloom Filter 判断 item 可能已经存在
    pub fn add(&mut self, item: &[u8]) -> Result<bool, BloomError> {
        let hash = bloom_hash(item);

        /*
         * RedisBloom:
         *
         * for (int ii = sb->nfilters - 1; ii >= 0; --ii) {
         *     if (bloom_check_h(...)) {
         *         return 0;
         *     }
         * }
         *
         * 必须先检查全部 filter。
         *
         * 不能只检查最后一层，否则：
         *
         * filter0 已经有 foo
         * filter1 没有 foo
         *
         * BF.ADD foo
         *
         * 会错误返回 1。
         */
        for filter in self.filters.iter().rev() {
            if filter.contains_hash(hash) {
                return Ok(false);
            }
        }

        // duplicate 检查必须在 overflow 检查之前。
        //
        // 即使 size 已经达到极限，一个已经存在的 item
        // 仍然应该返回 false，而不是插入错误。
        if self.size == u64::MAX {
            return Err(BloomError::Overflow);
        }

        let should_expand = {
            let current = self.filters.last().ok_or(BloomError::Invalid)?;

            current.size >= current.entries
        };

        if should_expand {
            if self.non_scaling {
                return Err(BloomError::Full);
            }

            let (next_capacity, next_error) = {
                let current = self.filters.last().ok_or(BloomError::Invalid)?;

                let next_capacity = current
                    .entries
                    .checked_mul(self.growth as u64)
                    .ok_or(BloomError::Overflow)?;

                let next_error = current.error * ERROR_TIGHTENING_RATIO;

                if next_capacity == 0 || next_error <= 0.0 || !next_error.is_finite() {
                    return Err(BloomError::Overflow);
                }

                (next_capacity, next_error)
            };

            /*
             * 先创建 next filter。
             *
             * 如果 bitmap 内存分配失败，不修改当前 BloomObject。
             */
            let next = BloomSubFilter::new(next_capacity, next_error)?;

            /*
             * 再保证 filters Vec 有空间。
             *
             * try_reserve 失败同样不会改变 Bloom 的逻辑内容。
             */
            self.filters
                .try_reserve(1)
                .map_err(|_| BloomError::OutOfMemory)?;

            self.filters.push(next);
        }

        let current = self.filters.last_mut().ok_or(BloomError::Invalid)?;

        /*
         * 到这里一定满足：
         *
         * current.size < current.entries
         *
         * 因此 current.size + 1 不会溢出。
         */
        let added = current.insert_hash(hash);

        if added {
            current.size += 1;
            self.size += 1;
        }

        Ok(added)
    }

    /// BF.EXISTS 后面可以直接复用。
    pub fn contains(&self, item: &[u8]) -> bool {
        let hash = bloom_hash(item);

        self.filters
            .iter()
            .rev()
            .any(|filter| filter.contains_hash(hash))
    }

    pub fn len(&self) -> u64 {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub fn total_capacity(&self) -> u64 {
        self.filters
            .iter()
            .fold(0u64, |acc, filter| acc.saturating_add(filter.entries))
    }

    pub fn bitmap_bytes(&self) -> usize {
        self.filters.iter().fold(0usize, |acc, filter| {
            acc.saturating_add(filter.bitmap.len())
        })
    }

    /// 不包括 BloomObject 自己 inline 的大小，
    /// 只统计它额外拥有的 heap allocation。
    pub fn estimated_heap_usage(&self) -> usize {
        /*
         * Vec<BloomSubFilter> allocation。
         *
         * 每个 BloomSubFilter 本身存在 Vec allocation 中，
         * 所以按 capacity * size_of::<BloomSubFilter>() 估算。
         */
        let filters_allocation = self
            .filters
            .capacity()
            .saturating_mul(size_of::<BloomSubFilter>());

        /*
         * 各个 bitmap backing allocation。
         */
        let bitmap_allocation = self.filters.iter().fold(0usize, |acc, filter| {
            acc.saturating_add(filter.bitmap.capacity())
        });

        filters_allocation.saturating_add(bitmap_allocation)
    }
}

impl BloomSubFilter {
    fn new(entries: u64, error: f64) -> Result<Self, BloomError> {
        if entries == 0 || !error.is_finite() || error <= 0.0 || error >= 1.0 {
            return Err(BloomError::Invalid);
        }

        /*
         * RedisBloom bloom.c:
         *
         * bpe = -log(error) / ln(2)^2
         */
        let bpe = -error.ln() / BPE_DENOM;

        if !bpe.is_finite() || bpe <= 0.0 {
            return Err(BloomError::Invalid);
        }

        /*
         * Redis BF 使用 BLOOM_OPT_NOROUND：
         *
         * bits = (uint64_t)(entries * bpe)
         */
        let raw_bits_float = entries as f64 * bpe;

        if !raw_bits_float.is_finite() || raw_bits_float >= u64::MAX as f64 {
            return Err(BloomError::Overflow);
        }

        let mut raw_bits = raw_bits_float as u64;

        if raw_bits == 0 {
            raw_bits = 1;
        }

        /*
         * RedisBloom FORCE64：
         *
         * if (bits % 64) {
         *     bytes = ((bits / 64) + 1) * 8;
         * } else {
         *     bytes = bits / 8;
         * }
         */
        let bytes_u64 = if raw_bits % 64 != 0 {
            raw_bits
                .checked_div(64)
                .and_then(|v| v.checked_add(1))
                .and_then(|v| v.checked_mul(8))
                .ok_or(BloomError::Overflow)?
        } else {
            raw_bits / 8
        };

        let bits = bytes_u64.checked_mul(8).ok_or(BloomError::Overflow)?;

        let bytes = usize::try_from(bytes_u64).map_err(|_| BloomError::Overflow)?;

        /*
         * RedisBloom:
         *
         * hashes = ceil(ln(2) * bpe)
         */
        let hashes_float = (LN2 * bpe).ceil();

        if !hashes_float.is_finite() || hashes_float < 1.0 || hashes_float > u32::MAX as f64 {
            return Err(BloomError::Invalid);
        }

        let hashes = hashes_float as u32;

        let mut bitmap = Vec::new();

        bitmap
            .try_reserve_exact(bytes)
            .map_err(|_| BloomError::OutOfMemory)?;

        bitmap.resize(bytes, 0);

        Ok(Self {
            bitmap,
            entries,
            size: 0,
            error,
            bpe,
            hashes,
            bits,
        })
    }

    fn contains_hash(&self, hash: BloomHash) -> bool {
        for i in 0..self.hashes as u64 {
            let bit = hash.a.wrapping_add(i.wrapping_mul(hash.b)) % self.bits;

            if !self.test_bit(bit) {
                return false;
            }
        }

        true
    }

    /// 返回 true 表示至少有一个 bit 原来为 0，
    /// 即 Bloom Filter 判断这是一个新元素。
    fn insert_hash(&mut self, hash: BloomHash) -> bool {
        let mut found_unset = false;

        for i in 0..self.hashes as u64 {
            let bit = hash.a.wrapping_add(i.wrapping_mul(hash.b)) % self.bits;

            if !self.test_and_set_bit(bit) {
                found_unset = true;
            }
        }

        found_unset
    }

    #[inline]
    fn test_bit(&self, bit: u64) -> bool {
        let byte_index = (bit >> 3) as usize;

        let bit_index = (bit & 7) as u32;

        let mask = 1u8 << bit_index;

        self.bitmap[byte_index] & mask != 0
    }

    /// 返回：
    ///
    /// true  => bit 之前已经是 1
    /// false => bit 之前是 0，现在被设置为 1
    #[inline]
    fn test_and_set_bit(&mut self, bit: u64) -> bool {
        let byte_index = (bit >> 3) as usize;

        let bit_index = (bit & 7) as u32;

        let mask = 1u8 << bit_index;

        let old = self.bitmap[byte_index];

        if old & mask != 0 {
            true
        } else {
            self.bitmap[byte_index] = old | mask;
            false
        }
    }
}

/// RedisBloom 使用两个 MurmurHash64A：
///
/// a = MurmurHash64A(item, fixed_seed)
/// b = MurmurHash64A(item, a)
///
/// 然后第 i 个 bit：
///
/// (a + i * b) % bits
fn bloom_hash(data: &[u8]) -> BloomHash {
    let a = murmur_hash64a(data, BLOOM_HASH_SEED);

    let b = murmur_hash64a(data, a);

    BloomHash { a, b }
}

/// MurmurHash64A。
///
/// 与 RedisBloom 使用的 MurmurHash64A_Bloom 算法一致。
///
/// 显式使用 little-endian，而不是直接把 &[u8] cast 成 *const u64，
/// 这样不同 little-endian CPU/对齐方式下结果都是确定的。
fn murmur_hash64a(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let mut hash = seed ^ (data.len() as u64).wrapping_mul(M);

    let mut offset = 0usize;

    while offset + 8 <= data.len() {
        let mut bytes = [0u8; 8];

        bytes.copy_from_slice(&data[offset..offset + 8]);

        let mut value = u64::from_le_bytes(bytes);

        value = value.wrapping_mul(M);
        value ^= value >> R;
        value = value.wrapping_mul(M);

        hash ^= value;
        hash = hash.wrapping_mul(M);

        offset += 8;
    }

    let tail = &data[offset..];

    match tail.len() {
        7 => {
            hash ^= (tail[6] as u64) << 48;
            hash ^= (tail[5] as u64) << 40;
            hash ^= (tail[4] as u64) << 32;
            hash ^= (tail[3] as u64) << 24;
            hash ^= (tail[2] as u64) << 16;
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        6 => {
            hash ^= (tail[5] as u64) << 40;
            hash ^= (tail[4] as u64) << 32;
            hash ^= (tail[3] as u64) << 24;
            hash ^= (tail[2] as u64) << 16;
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        5 => {
            hash ^= (tail[4] as u64) << 32;
            hash ^= (tail[3] as u64) << 24;
            hash ^= (tail[2] as u64) << 16;
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        4 => {
            hash ^= (tail[3] as u64) << 24;
            hash ^= (tail[2] as u64) << 16;
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        3 => {
            hash ^= (tail[2] as u64) << 16;
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        2 => {
            hash ^= (tail[1] as u64) << 8;
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        1 => {
            hash ^= tail[0] as u64;
            hash = hash.wrapping_mul(M);
        }

        _ => {}
    }

    hash ^= hash >> R;

    hash = hash.wrapping_mul(M);

    hash ^= hash >> R;

    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn redis_default_layout() {
        let bloom = BloomObject::redis_default().unwrap();

        assert_eq!(bloom.filters.len(), 1);

        let first = &bloom.filters[0];

        assert_eq!(first.entries, 100);

        // scalable filter 第一层使用 0.01 * 0.5
        assert!((first.error - 0.005).abs() < f64::EPSILON);

        // RedisBloom FORCE64 + NOROUND
        assert_eq!(first.bits, 1152);

        assert_eq!(first.bitmap.len(), 144);

        assert_eq!(first.hashes, 8);
    }

    #[test]
    fn add_same_item_twice() {
        let mut bloom = BloomObject::redis_default().unwrap();

        assert_eq!(bloom.add(b"hello").unwrap(), true);

        assert_eq!(bloom.add(b"hello").unwrap(), false);

        assert_eq!(bloom.len(), 1);

        assert!(bloom.contains(b"hello"));
    }

    #[test]
    fn binary_safe() {
        let mut bloom = BloomObject::redis_default().unwrap();

        let item = b"\x00\xff\x10hello\x00";

        assert!(bloom.add(item).unwrap());

        assert!(bloom.contains(item));

        assert!(!bloom.add(item).unwrap());
    }

    #[test]
    fn scalable_filter_expands() {
        let mut bloom = BloomObject::redis_default().unwrap();

        let mut i = 0u64;

        /*
         * Bloom Filter 存在 false positive，
         * 所以不能简单假设前 101 个不同字符串
         * 一定全部返回 true。
         */
        while bloom.filter_count() == 1 {
            let item = format!("item:{i}");

            bloom.add(item.as_bytes()).unwrap();

            i += 1;

            assert!(i < 100_000);
        }

        assert_eq!(bloom.filter_count(), 2);

        assert_eq!(bloom.filters[0].entries, 100);

        assert_eq!(bloom.filters[1].entries, 200);

        assert!((bloom.filters[0].error - 0.005).abs() < f64::EPSILON);

        assert!((bloom.filters[1].error - 0.0025).abs() < f64::EPSILON);
    }

    #[test]
    fn no_false_negative() {
        let mut bloom = BloomObject::redis_default().unwrap();

        let mut inserted = Vec::new();

        for i in 0..1000 {
            let item = format!("foo:{i}");

            if bloom.add(item.as_bytes()).unwrap() {
                inserted.push(item);
            }
        }

        for item in inserted {
            assert!(bloom.contains(item.as_bytes()));
        }
    }

    #[test]
    fn mutex_serializes_concurrent_add() {
        let bloom = Arc::new(Mutex::new(BloomObject::redis_default().unwrap()));

        let mut handles = Vec::new();

        for _ in 0..16 {
            let bloom = bloom.clone();

            handles.push(thread::spawn(move || {
                let mut guard = bloom.lock();

                guard.add(b"same-value").unwrap()
            }));
        }

        let inserted_count = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|added| *added)
            .count();

        /*
         * 所有线程都加同一个 value。
         *
         * 在 Mutex 保护下：
         *
         * 第一个 => true
         * 其他   => false
         */
        assert_eq!(inserted_count, 1);
    }
}
