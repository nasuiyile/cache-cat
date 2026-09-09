use std::{fmt, ops::Bound, str::FromStr};
use crate::raft::types::core::structure::stream::types::StreamError;

/// A Redis stream ID, ordered numerically by (milliseconds, sequence).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: Self = Self { ms: 0, seq: 0 };
    pub const MAX: Self = Self { ms: u64::MAX, seq: u64::MAX };

    pub const fn new(ms: u64, seq: u64) -> Self { Self { ms, seq } }

    /// Fixed-width, prefix-free, lexicographically order-preserving encoding.
    pub fn to_key(self) -> [u8; 16] {
        let mut key = [0; 16];
        key[..8].copy_from_slice(&self.ms.to_be_bytes());
        key[8..].copy_from_slice(&self.seq.to_be_bytes());
        key
    }

    pub fn from_key(key: [u8; 16]) -> Self {
        let mut ms = [0; 8];
        let mut seq = [0; 8];
        ms.copy_from_slice(&key[..8]);
        seq.copy_from_slice(&key[8..]);
        Self::new(u64::from_be_bytes(ms), u64::from_be_bytes(seq))
    }

    pub fn successor(self) -> Option<Self> {
        match self.seq.checked_add(1) {
            Some(seq) => Some(Self::new(self.ms, seq)),
            None => self.ms.checked_add(1).map(|ms| Self::new(ms, 0)),
        }
    }

    pub fn predecessor(self) -> Option<Self> {
        match self.seq.checked_sub(1) {
            Some(seq) => Some(Self::new(self.ms, seq)),
            None => self.ms.checked_sub(1).map(|ms| Self::new(ms, u64::MAX)),
        }
    }

    fn parse_with_default(s: &str, default_seq: u64) -> Result<Self, StreamError> {
        match s.split_once('-') {
            Some((ms, seq)) => Ok(Self::new(decimal(ms)?, decimal(seq)?)),
            None => Ok(Self::new(decimal(s)?, default_seq)),
        }
    }
}

fn decimal(s: &str) -> Result<u64, StreamError> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return Err(StreamError::InvalidId);
    }
    s.parse().map_err(|_| StreamError::InvalidId)
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for StreamId {
    type Err = StreamError;
    /// A bare timestamp is interpreted as timestamp-0.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_with_default(s, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AddId {
    Auto,
    AutoSequence(u64),
    Explicit(StreamId),
}

impl FromStr for AddId {
    type Err = StreamError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" { return Ok(Self::Auto); }
        if let Some(ms) = s.strip_suffix("-*") {
            return Ok(Self::AutoSequence(decimal(ms)?));
        }
        Ok(Self::Explicit(s.parse()?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStart {
    Id(StreamId),
    /// Resolve `$` to the last generated ID at the time of the operation.
    Last,
}

impl FromStr for GroupStart {
    type Err = StreamError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "$" { Ok(Self::Last) } else { Ok(Self::Id(s.parse()?)) }
    }
}

/// Bounds are in ascending order even when used with `xrevrange`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdRange {
    pub start: Bound<StreamId>,
    pub end: Bound<StreamId>,
}

impl Default for IdRange {
    fn default() -> Self { Self::all() }
}

impl IdRange {
    pub const fn all() -> Self {
        Self { start: Bound::Unbounded, end: Bound::Unbounded }
    }

    pub const fn inclusive(start: StreamId, end: StreamId) -> Self {
        Self { start: Bound::Included(start), end: Bound::Included(end) }
    }

    pub const fn after(id: StreamId) -> Self {
        Self { start: Bound::Excluded(id), end: Bound::Unbounded }
    }

    /// Parses XRANGE-style bounds, including `-`, `+`, `(id` and bare ms.
    /// Missing sequence: 0 for the lower bound, u64::MAX for the upper bound.
    pub fn parse(start: &str, end: &str) -> Result<Self, StreamError> {
        fn bound(s: &str, seq: u64) -> Result<Bound<StreamId>, StreamError> {
            let (exclusive, text) = match s.strip_prefix('(') {
                Some(text) => (true, text),
                None => (false, s),
            };
            let id = match text {
                "-" => StreamId::ZERO,
                "+" => StreamId::MAX,
                _ => StreamId::parse_with_default(text, seq)?,
            };
            Ok(if exclusive { Bound::Excluded(id) } else { Bound::Included(id) })
        }
        Ok(Self { start: bound(start, 0)?, end: bound(end, u64::MAX)? })
    }

    /// None means an empty interval. Avoid passing invalid bounds to blart.
    pub(crate) fn normalized(self) -> Option<(StreamId, StreamId)> {
        let lo = match self.start {
            Bound::Included(id) => id,
            Bound::Excluded(id) => id.successor()?,
            Bound::Unbounded => StreamId::ZERO,
        };
        let hi = match self.end {
            Bound::Included(id) => id,
            Bound::Excluded(id) => id.predecessor()?,
            Bound::Unbounded => StreamId::MAX,
        };
        (lo <= hi).then_some((lo, hi))
    }
}
