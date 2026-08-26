use bytes::Bytes;
use std::convert::TryInto;

const HLL_P: u32 = 14;
const HLL_Q: u32 = 64 - HLL_P;

const HLL_REGISTERS: usize = 1usize << HLL_P;
const HLL_BITS: usize = 6;

const HLL_HDR_SIZE: usize = 16;

const HLL_DENSE_BYTES: usize =
    (HLL_REGISTERS * HLL_BITS + 7) / 8;

const HLL_DENSE_SIZE: usize =
    HLL_HDR_SIZE + HLL_DENSE_BYTES;

const HLL_DENSE: u8 = 0;
const HLL_SPARSE: u8 = 1;
const HLL_MAX_ENCODING: u8 = HLL_SPARSE;

// Redis 默认配置。
// 如果你以后把 hll-sparse-max-bytes 暴露成配置项，
// 可以把这个常量改成参数。
const HLL_SPARSE_MAX_BYTES: usize = 3000;

const HLL_SPARSE_VAL_MAX: u8 = 32;
const HLL_SPARSE_VAL_MAX_LEN: usize = 4;
const HLL_SPARSE_ZERO_MAX_LEN: usize = 64;
const HLL_SPARSE_XZERO_MAX_LEN: usize = 16384;

const HLL_HASH_SEED: u64 = 0xadc83b19;

const HLL_ALPHA_INF: f64 = 0.721347520444481703680;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HllDecodeError {
    /// 不是 Redis HLL。
    ///
    /// 对应：
    /// WRONGTYPE Key is not a valid HyperLogLog string value.
    NotHll,

    /// header 看起来是 HLL，但 sparse 数据损坏。
    ///
    /// 对应：
    /// INVALIDOBJ Corrupted HLL object detected
    Corrupted,
}

/// Redis-compatible HyperLogLog。
///
/// 内部使用一个 byte/register 的 RAW 表示，方便在不可变 Bytes
/// 架构中修改；持久化时再编码回 Redis sparse/dense 格式。
#[derive(Debug, Clone)]
pub struct RedisHll {
    registers: Vec<u8>,

    // header[5..8]
    reserved: [u8; 3],

    // header[8..16]
    card: [u8; 8],

    // Redis 不会把 dense 自动降级成 sparse。
    // 保留这个信息可以更接近 Redis 的内部行为。
    prefer_dense: bool,
}

impl RedisHll {
    /// 创建一个新的空 HLL。
    pub fn new() -> Self {
        Self {
            registers: vec![0; HLL_REGISTERS],
            reserved: [0; 3],
            card: [0; 8],
            prefer_dense: false,
        }
    }

    /// 只执行 Redis isHLLObjectOrReply() 类似的 header 检查。
    ///
    /// PFADD key 没有 element 时非常重要：
    /// Redis 不会深入解析 sparse body。
    pub fn validate_header(raw: &[u8]) -> Result<(), HllDecodeError> {
        if raw.len() < HLL_HDR_SIZE {
            return Err(HllDecodeError::NotHll);
        }

        if &raw[0..4] != b"HYLL" {
            return Err(HllDecodeError::NotHll);
        }

        let encoding = raw[4];

        if encoding > HLL_MAX_ENCODING {
            return Err(HllDecodeError::NotHll);
        }

        // Redis 对 dense 长度要求严格等于 12304。
        if encoding == HLL_DENSE && raw.len() != HLL_DENSE_SIZE {
            return Err(HllDecodeError::NotHll);
        }

        Ok(())
    }

    /// 解码 Redis String 里的 HLL。
    pub fn decode(raw: &[u8]) -> Result<Self, HllDecodeError> {
        Self::validate_header(raw)?;

        let mut hll = Self::new();

        hll.reserved.copy_from_slice(&raw[5..8]);
        hll.card.copy_from_slice(&raw[8..16]);

        match raw[4] {
            HLL_DENSE => {
                hll.prefer_dense = true;
                hll.decode_dense(raw)?;
            }

            HLL_SPARSE => {
                hll.prefer_dense = false;
                hll.decode_sparse(raw)?;
            }

            _ => {
                return Err(HllDecodeError::NotHll);
            }
        }

        Ok(hll)
    }

    fn decode_dense(&mut self, raw: &[u8]) -> Result<(), HllDecodeError> {
        if raw.len() != HLL_DENSE_SIZE {
            return Err(HllDecodeError::NotHll);
        }

        for index in 0..HLL_REGISTERS {
            self.registers[index] = dense_get_register(raw, index);
        }

        Ok(())
    }

    fn decode_sparse(&mut self, raw: &[u8]) -> Result<(), HllDecodeError> {
        let mut pos = HLL_HDR_SIZE;
        let mut reg = 0usize;

        while pos < raw.len() {
            let opcode = raw[pos];

            /*
             * ZERO:
             *
             * 00xxxxxx
             *
             * length = xxxxxx + 1
             */
            if opcode & 0xc0 == 0 {
                let run = ((opcode & 0x3f) as usize) + 1;

                if reg + run > HLL_REGISTERS {
                    return Err(HllDecodeError::Corrupted);
                }

                // register 本来就是 0，无需写入。
                reg += run;
                pos += 1;

                continue;
            }

            /*
             * XZERO:
             *
             * 01xxxxxx yyyyyyyy
             */
            if opcode & 0xc0 == 0x40 {
                if pos + 1 >= raw.len() {
                    return Err(HllDecodeError::Corrupted);
                }

                let run = ((((opcode & 0x3f) as usize) << 8)
                    | raw[pos + 1] as usize)
                    + 1;

                if reg + run > HLL_REGISTERS {
                    return Err(HllDecodeError::Corrupted);
                }

                reg += run;
                pos += 2;

                continue;
            }

            /*
             * VAL:
             *
             * 1vvvvvxx
             *
             * value = vvvvv + 1
             * len   = xx + 1
             */
            let value = ((opcode >> 2) & 0x1f) + 1;
            let run = ((opcode & 0x03) as usize) + 1;

            if reg + run > HLL_REGISTERS {
                return Err(HllDecodeError::Corrupted);
            }

            for index in reg..reg + run {
                self.registers[index] = value;
            }

            reg += run;
            pos += 1;
        }

        if reg != HLL_REGISTERS {
            return Err(HllDecodeError::Corrupted);
        }

        Ok(())
    }

    /// 添加一个二进制 element。
    ///
    /// true  = 至少一个 register 被提高
    /// false = 没变化
    pub fn add(&mut self, element: &[u8]) -> bool {
        let (index, count) = hll_pattern(element);

        let old = self.registers[index];

        if count <= old {
            return false;
        }

        self.registers[index] = count;
        true
    }

    /// Redis 在 HLL 更新之后把 cached cardinality 标记为 invalid。
    pub fn invalidate_cache(&mut self) {
        self.card[7] |= 0x80;
    }

    /// 编码成 Redis String 可以直接保存的 Bytes。
    pub fn into_bytes(self) -> Bytes {
        // 已经是 dense 的 Redis HLL 不主动降级。
        if !self.prefer_dense {
            if let Some(sparse) = self.encode_sparse() {
                return Bytes::from(sparse);
            }
        }

        Bytes::from(self.encode_dense())
    }

    fn encode_dense(&self) -> Vec<u8> {
        let mut out = vec![0u8; HLL_DENSE_SIZE];

        self.write_header(&mut out, HLL_DENSE);

        for (index, &value) in self.registers.iter().enumerate() {
            if value != 0 {
                dense_set_register(&mut out, index, value);
            }
        }

        out
    }

    /// 尝试编码为 Redis sparse HLL。
    ///
    /// 如果：
    /// - register > 32
    /// - 或结果超过 hll-sparse-max-bytes
    ///
    /// 就返回 None，调用方升级为 dense。
    fn encode_sparse(&self) -> Option<Vec<u8>> {
        let mut body = Vec::with_capacity(256);

        let mut index = 0usize;

        while index < HLL_REGISTERS {
            let value = self.registers[index];

            if value == 0 {
                let start = index;

                while index < HLL_REGISTERS
                    && self.registers[index] == 0
                {
                    index += 1;
                }

                let mut remaining = index - start;

                while remaining != 0 {
                    if remaining > HLL_SPARSE_ZERO_MAX_LEN {
                        let run = remaining.min(HLL_SPARSE_XZERO_MAX_LEN);

                        push_xzero(&mut body, run);

                        remaining -= run;
                    } else {
                        let run = remaining.min(HLL_SPARSE_ZERO_MAX_LEN);

                        push_zero(&mut body, run);

                        remaining -= run;
                    }

                    if HLL_HDR_SIZE + body.len() > HLL_SPARSE_MAX_BYTES {
                        return None;
                    }
                }
            } else {
                if value > HLL_SPARSE_VAL_MAX {
                    return None;
                }

                let mut run = 1usize;

                while run < HLL_SPARSE_VAL_MAX_LEN
                    && index + run < HLL_REGISTERS
                    && self.registers[index + run] == value
                {
                    run += 1;
                }

                push_val(&mut body, value, run);

                index += run;

                if HLL_HDR_SIZE + body.len() > HLL_SPARSE_MAX_BYTES {
                    return None;
                }
            }
        }

        let mut out = vec![0u8; HLL_HDR_SIZE];

        self.write_header(&mut out, HLL_SPARSE);

        out.extend_from_slice(&body);

        Some(out)
    }

    fn write_header(&self, out: &mut [u8], encoding: u8) {
        out[0..4].copy_from_slice(b"HYLL");
        out[4] = encoding;
        out[5..8].copy_from_slice(&self.reserved);
        out[8..16].copy_from_slice(&self.card);
    }
    /// 读取 Redis HLL header 中缓存的 cardinality。
    ///
    /// Redis header:
    ///
    /// [0..4]   "HYLL"
    /// [4]      encoding
    /// [5..8]   reserved
    /// [8..16]  cached cardinality, little endian
    ///
    /// card[7] 的最高位为 1 表示 cache invalid。
    ///
    /// 注意：
    /// 这里只验证 HLL header，不解析 sparse body。
    /// 这与 Redis PFCOUNT 单 key 的行为一致：
    /// 如果 cache 有效，Redis 会直接返回 cache。
    pub fn cached_cardinality(
        raw: &[u8],
    ) -> Result<Option<u64>, HllDecodeError> {
        Self::validate_header(raw)?;

        // HLL_INVALIDATE_CACHE:
        //
        // hdr->card[7] |= (1 << 7)
        if raw[15] & 0x80 != 0 {
            return Ok(None);
        }

        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&raw[8..16]);

        Ok(Some(u64::from_le_bytes(bytes)))
    }

    pub fn merge(&mut self, other: &RedisHll) {
        // Redis PFMERGE:
        //
        // If at least one involved HLL is dense,
        // use dense representation for destination.
        if other.prefer_dense {
            self.prefer_dense = true;
        }

        for (dst, src) in self
            .registers
            .iter_mut()
            .zip(other.registers.iter())
        {
            if *src > *dst {
                *dst = *src;
            }
        }
    }

    /// 计算当前 HLL 的估算 cardinality。
    ///
    /// 不读取/修改 cached cardinality。
    pub fn cardinality(&self) -> u64 {
        hll_count(&self.registers)
    }
}

impl Default for RedisHll {
    fn default() -> Self {
        Self::new()
    }
}

fn push_zero(out: &mut Vec<u8>, len: usize) {
    debug_assert!((1..=HLL_SPARSE_ZERO_MAX_LEN).contains(&len));

    out.push((len - 1) as u8);
}

fn push_xzero(out: &mut Vec<u8>, len: usize) {
    debug_assert!((1..=HLL_SPARSE_XZERO_MAX_LEN).contains(&len));

    let value = len - 1;

    out.push(
        0x40 | (((value >> 8) & 0x3f) as u8)
    );

    out.push((value & 0xff) as u8);
}

fn push_val(out: &mut Vec<u8>, value: u8, len: usize) {
    debug_assert!((1..=HLL_SPARSE_VAL_MAX).contains(&value));
    debug_assert!((1..=HLL_SPARSE_VAL_MAX_LEN).contains(&len));

    let opcode =
        0x80
            | ((value - 1) << 2)
            | ((len - 1) as u8);

    out.push(opcode);
}

/// Redis dense HLL:
///
/// 16384 registers × 6 bits。
fn dense_get_register(raw: &[u8], index: usize) -> u8 {
    let bit = index * HLL_BITS;

    let byte_index =
        HLL_HDR_SIZE + bit / 8;

    let shift =
        bit & 7;

    let mut word =
        raw[byte_index] as u16;

    if shift + HLL_BITS > 8 {
        word |=
            (raw[byte_index + 1] as u16) << 8;
    }

    ((word >> shift) & 0x3f) as u8
}

fn dense_set_register(
    raw: &mut [u8],
    index: usize,
    value: u8,
) {
    debug_assert!(index < HLL_REGISTERS);
    debug_assert!(value <= 63);

    let bit = index * HLL_BITS;

    let byte_index =
        HLL_HDR_SIZE + bit / 8;

    let shift =
        bit & 7;

    let spans_two_bytes =
        shift + HLL_BITS > 8;

    let mut word =
        raw[byte_index] as u16;

    if spans_two_bytes {
        word |=
            (raw[byte_index + 1] as u16) << 8;
    }

    let mask =
        0x3fu16 << shift;

    word =
        (word & !mask)
            | ((value as u16) << shift);

    raw[byte_index] =
        word as u8;

    if spans_two_bytes {
        raw[byte_index + 1] =
            (word >> 8) as u8;
    }
}

/// Redis hllPatLen().
fn hll_pattern(element: &[u8]) -> (usize, u8) {
    let mut hash =
        murmur_hash64a(element);

    // 低 14 bit 决定 register。
    let index =
        (hash & ((1u64 << HLL_P) - 1)) as usize;

    hash >>= HLL_P;

    // Redis 保证 trailing-zero 搜索一定终止。
    hash |=
        1u64 << HLL_Q;

    let count =
        hash.trailing_zeros() as u8 + 1;

    (index, count)
}

/// Redis HyperLogLog 使用的 MurmurHash64A。
///
/// 注意 wrapping_mul 很重要：
/// C 的 unsigned overflow 就是 modulo 2^64。
fn murmur_hash64a(data: &[u8]) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;

    let mut hash =
        HLL_HASH_SEED
            ^ (data.len() as u64).wrapping_mul(M);

    let mut pos = 0usize;

    while pos + 8 <= data.len() {
        // Redis 为了跨端序一致，把块按照 little-endian 解释。
        let mut value = u64::from_le_bytes(
            data[pos..pos + 8]
                .try_into()
                .expect("8 byte chunk"),
        );

        value =
            value.wrapping_mul(M);

        value ^= value >> R;

        value =
            value.wrapping_mul(M);

        hash ^= value;

        hash =
            hash.wrapping_mul(M);

        pos += 8;
    }

    let tail =
        &data[pos..];

    if !tail.is_empty() {
        for (i, &byte) in tail.iter().enumerate() {
            hash ^= (byte as u64) << (i * 8);
        }

        hash =
            hash.wrapping_mul(M);
    }

    hash ^= hash >> R;

    hash =
        hash.wrapping_mul(M);

    hash ^= hash >> R;

    hash
}
/// Redis hllSigma().
fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }

    let mut y = 1.0;
    let mut z = x;

    loop {
        x *= x;

        let z_prime = z;

        z += x * y;

        y += y;

        if z_prime == z {
            break;
        }
    }

    z
}

/// Redis hllTau().
fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }

    let mut y = 1.0;
    let mut z = 1.0 - x;

    loop {
        x = x.sqrt();

        let z_prime = z;

        y *= 0.5;

        let d = 1.0 - x;

        z -= d * d * y;

        if z_prime == z {
            break;
        }
    }

    z / 3.0
}

/// Redis hllCount()。
///
/// registers 使用我们的 RAW 表示：
///
///     16384 bytes
///     每个 byte 一个 register
///
/// 这实际上对应 Redis 内部 PFCOUNT multi-key 使用的
/// HLL_RAW representation。
fn hll_count(registers: &[u8]) -> u64 {
    debug_assert_eq!(
        registers.len(),
        HLL_REGISTERS
    );

    /*
     * Redis 使用 64 项 histogram。
     *
     * register 是 6 bit：
     *
     *     0..=63
     */
    let mut histogram = [0u64; 64];

    for &register in registers {
        histogram[register as usize] += 1;
    }

    let m = HLL_REGISTERS as f64;

    /*
     * Redis:
     *
     * double z =
     *     m * hllTau(
     *         (m - reghisto[HLL_Q + 1]) / m
     *     );
     *
     * HLL_Q = 64 - 14 = 50
     *
     * 所以 HLL_Q + 1 = 51。
     */
    let mut z =
        m * hll_tau(
            (m - histogram[(HLL_Q + 1) as usize] as f64)
                / m,
        );

    /*
     * Redis:
     *
     * for (j = HLL_Q; j >= 1; --j) {
     *     z += reghisto[j];
     *     z *= 0.5;
     * }
     */
    for j in (1..=HLL_Q as usize).rev() {
        z += histogram[j] as f64;
        z *= 0.5;
    }

    /*
     * Redis:
     *
     * z += m * hllSigma(reghisto[0] / m);
     */
    z += m * hll_sigma(
        histogram[0] as f64 / m
    );

    /*
     * Redis:
     *
     * E = llroundl(
     *     HLL_ALPHA_INF * m * m / z
     * );
     */
    let estimate =
        HLL_ALPHA_INF * m * m / z;

    estimate.round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_hash_vectors() {
        assert_eq!(
            murmur_hash64a(b"a"),
            0x53d2470a9b43b1a7
        );

        assert_eq!(
            murmur_hash64a(b"b"),
            0xf10cdf96c004fda4
        );

        assert_eq!(
            murmur_hash64a(b"hello"),
            0x0f656f01eecfe400
        );

        assert_eq!(
            hll_pattern(b"a"),
            (12711, 2)
        );

        assert_eq!(
            hll_pattern(b"b"),
            (15780, 1)
        );
    }

    #[test]
    fn same_value_only_updates_once() {
        let mut hll = RedisHll::new();

        assert!(hll.add(b"hello"));
        assert!(!hll.add(b"hello"));
    }

    #[test]
    fn empty_hll_is_redis_sparse_representation() {
        let hll = RedisHll::new();

        let bytes = hll.into_bytes();

        assert_eq!(&bytes[0..4], b"HYLL");
        assert_eq!(bytes[4], HLL_SPARSE);

        // Empty Redis HLL:
        // header(16) + XZERO(16384)(2)
        assert_eq!(bytes.len(), 18);

        assert_eq!(bytes[16], 0x7f);
        assert_eq!(bytes[17], 0xff);
    }

    #[test]
    fn redis_sparse_round_trip() {
        let mut raw = vec![0u8; 18];

        raw[0..4].copy_from_slice(b"HYLL");
        raw[4] = HLL_SPARSE;

        raw[16] = 0x7f;
        raw[17] = 0xff;

        let mut hll =
            RedisHll::decode(&raw).unwrap();

        assert!(hll.add(b"foo"));
        assert!(!hll.add(b"foo"));

        let encoded =
            hll.into_bytes();

        assert_eq!(&encoded[0..4], b"HYLL");
    }
}