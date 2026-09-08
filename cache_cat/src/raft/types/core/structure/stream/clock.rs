use std::sync::{atomic::{AtomicU64, Ordering}, Arc};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::raft::types::core::structure::stream::memory;

/// A command samples the clock once. Values are Unix milliseconds.
/// Custom clocks must not panic or re-enter the stream.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;

    /// Extra heap bytes owned/referenced by the clock beyond its inline value.
    /// The stream counts the outer Arc allocation separately. Shared subobjects
    /// are charged in full. Override for custom clocks with heap storage.
    fn estimated_heap_bytes(&self) -> usize { 0 }
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let ms = SystemTime::now().duration_since(UNIX_EPOCH)
            .unwrap_or_default().as_millis();
        ms.min(u64::MAX as u128) as u64
    }
}

/// A shared, controllable clock for deterministic tests and replay.
#[derive(Debug, Clone, Default)]
pub struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(now_ms)))
    }

    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }

    /// Advances without wrapping at u64::MAX.
    pub fn advance(&self, elapsed_ms: u64) {
        let _ = self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst,
            |old| Some(old.saturating_add(elapsed_ms)));
    }
}

impl Clock for ManualClock {
    fn estimated_heap_bytes(&self) -> usize {
        memory::arc_bytes::<AtomicU64>()
    }

    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
