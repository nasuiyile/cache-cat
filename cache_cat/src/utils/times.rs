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
