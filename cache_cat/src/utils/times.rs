use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Get the current timestamp in milliseconds
#[inline(always)]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
#[inline(always)]
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
#[inline(always)]
pub fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// 相差多少秒
#[inline(always)]
pub fn time_gap(old_time: u64) -> u64 {
    now_ms().saturating_sub(old_time) / 1000
}

/// Redis stores expiration deadlines in a signed 64-bit millisecond value.
/// The cache uses `u64` for its logical clock, so keep the Redis range check
/// at the conversion boundary instead of allowing a wrapped deadline.
#[inline]
pub(crate) fn checked_redis_deadline(now: u64, delay: u64) -> Option<u64> {
    now.checked_add(delay)
        .filter(|deadline| *deadline <= i64::MAX as u64)
}

#[inline]
pub(crate) fn checked_redis_timestamp(timestamp: u64) -> Option<u64> {
    (timestamp <= i64::MAX as u64).then_some(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_deadline_stays_within_signed_millisecond_range() {
        let max = i64::MAX as u64;
        assert_eq!(checked_redis_deadline(max - 1, 1), Some(max));
        assert_eq!(checked_redis_deadline(max, 1), None);
        assert_eq!(checked_redis_timestamp(max), Some(max));
        assert_eq!(checked_redis_timestamp(max + 1), None);
    }
}
