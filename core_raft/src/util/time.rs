//! Time utility functions

use std::time::{SystemTime, UNIX_EPOCH};

/// Get the current timestamp in milliseconds
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_now_ms() {
        let t1 = now_ms();
        thread::sleep(Duration::from_millis(10));
        let t2 = now_ms();
        assert!(t2 > t1);
    }
}
