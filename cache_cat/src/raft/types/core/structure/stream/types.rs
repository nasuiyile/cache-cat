use crate::raft::types::core::structure::stream::id::{IdRange, StreamId};
use serde::{Deserialize, Serialize};
use std::fmt;

pub type Fields = Vec<(Vec<u8>, Vec<u8>)>;
pub type Result<T> = std::result::Result<T, StreamError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    InvalidId,
    ZeroId,
    IdNotIncreasing,
    IdExhausted,
    EmptyFields,
    GroupExists,
    NoGroup,
    GroupRecreated,
    InvalidCount,
    InvalidMetadata,
    CounterOverflow,
    LockPoisoned,
    InvalidTimeout,
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidId => "invalid stream ID",
            Self::ZeroId => "XADD ID must be greater than 0-0",
            Self::IdNotIncreasing => "XADD ID must exceed the last generated ID",
            Self::IdExhausted => "stream ID space or fixed timestamp sequence exhausted",
            Self::EmptyFields => "an entry must have at least one field-value pair",
            Self::GroupExists => "BUSYGROUP consumer group already exists",
            Self::NoGroup => "NOGROUP consumer group does not exist",
            Self::GroupRecreated => "consumer group was destroyed and recreated while blocked",
            Self::InvalidCount => "count must be positive and within the supported range",
            Self::InvalidMetadata => "metadata would violate stream invariants",
            Self::CounterOverflow => "stream counter exhausted",
            Self::LockPoisoned => "stream state was poisoned by a panicking mutation",
            Self::InvalidTimeout => "timeout exceeds the monotonic clock range",
        })
    }
}
impl std::error::Error for StreamError {}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    pub id: StreamId,
    /// Ordered, binary-safe pairs; duplicate field names are preserved.
    pub fields: Fields,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupEntry {
    pub id: StreamId,
    /// None represents a PEL record whose stream payload has been deleted.
    pub fields: Option<Fields>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReferencePolicy {
    #[default]
    KeepRef,
    DelRef,
    Acked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimStrategy {
    MaxLen(usize),
    MinId(StreamId),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TrimMode {
    #[default]
    Exact,
    /// Per-entry fallback, NOT Redis listpack/macro-node trimming.
    /// None uses 10,000 examined entries; Some(0) means unlimited.
    Approximate { limit: Option<usize> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimOptions {
    pub strategy: TrimStrategy,
    pub mode: TrimMode,
    pub references: ReferencePolicy,
}

impl TrimOptions {
    pub fn max_len(len: usize) -> Self {
        Self {
            strategy: TrimStrategy::MaxLen(len),
            mode: TrimMode::Exact,
            references: ReferencePolicy::KeepRef,
        }
    }
    pub fn min_id(id: StreamId) -> Self {
        Self {
            strategy: TrimStrategy::MinId(id),
            mode: TrimMode::Exact,
            references: ReferencePolicy::KeepRef,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AddOptions {
    pub trim: Option<TrimOptions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStart {
    After(StreamId),
    /// `$`: resolve once on entry to a blocking operation.
    Tail,
    /// `+`: return the last live entry, ignoring count; do not wait.
    Latest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupRead {
    New,
    PendingAfter(StreamId),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadGroupOptions {
    /// None means unlimited; Some(0) returns no entries (typed API convention).
    pub count: Option<usize>,
    pub no_ack: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeliveryTime {
    #[default]
    Now,
    IdleMs(u64),
    UnixMs(u64),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClaimOptions {
    pub min_idle_ms: u64,
    pub force: bool,
    pub just_id: bool,
    pub delivery_time: DeliveryTime,
    pub retry_count: Option<u64>,
    pub last_id: Option<StreamId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoClaimOptions {
    pub min_idle_ms: u64,
    pub start: StreamId,
    pub count: usize,
    pub just_id: bool,
}
impl Default for AutoClaimOptions {
    fn default() -> Self {
        Self {
            min_idle_ms: 0,
            start: StreamId::ZERO,
            count: 100,
            just_id: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimReply {
    Entries(Vec<Entry>),
    Ids(Vec<StreamId>),
}
impl ClaimReply {
    pub fn len(&self) -> usize {
        match self {
            Self::Entries(v) => v.len(),
            Self::Ids(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn ids(&self) -> Vec<StreamId> {
        match self {
            Self::Entries(v) => v.iter().map(|e| e.id).collect(),
            Self::Ids(v) => v.clone(),
        }
    }
    pub(crate) fn empty(just_id: bool) -> Self {
        if just_id {
            Self::Ids(Vec::new())
        } else {
            Self::Entries(Vec::new())
        }
    }
    pub(crate) fn push(&mut self, id: StreamId, fields: &Fields) {
        match self {
            Self::Ids(v) => v.push(id),
            Self::Entries(v) => v.push(Entry {
                id,
                fields: fields.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoClaimReply {
    /// ZERO indicates the current scan reached the end, not that the PEL is empty.
    pub next_start: StreamId,
    pub claimed: ClaimReply,
    pub deleted_ids: Vec<StreamId>,
}

#[derive(Debug, Clone, Copy)]
pub struct PendingQuery<'a> {
    pub range: IdRange,
    pub count: usize,
    pub consumer: Option<&'a [u8]>,
    pub min_idle_ms: Option<u64>,
}
impl Default for PendingQuery<'_> {
    fn default() -> Self {
        Self {
            range: IdRange::all(),
            count: 100,
            consumer: None,
            min_idle_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInfo {
    pub id: StreamId,
    pub consumer: Vec<u8>,
    pub idle_ms: u64,
    pub deliveries: u64,
    pub last_delivery_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSummary {
    pub total: usize,
    pub min_id: Option<StreamId>,
    pub max_id: Option<StreamId>,
    /// Only consumers with pending entries; sorted by binary name.
    pub consumers: Vec<(Vec<u8>, usize)>,
}

/// -1 / 1 / 2, matching the Redis 8.2 status categories.
/// For XACKDEL, NotFound means "not in the specified group's PEL".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum DeleteStatus {
    NotFound = -1,
    Deleted = 1,
    StillReferenced = 2,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SetIdOptions {
    pub entries_added: Option<u64>,
    /// ZERO leaves the existing tombstone marker unchanged.
    pub max_deleted_id: Option<StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub length: usize,
    pub groups: usize,
    pub last_generated_id: StreamId,
    pub max_deleted_entry_id: StreamId,
    pub entries_added: u64,
    pub recorded_first_entry_id: StreamId,
    pub first_entry: Option<Entry>,
    pub last_entry: Option<Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupInfo {
    pub name: Vec<u8>,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,
    /// Redis-style logical lag; None when the counter cannot be inferred safely.
    pub lag: Option<u64>,
    /// Library extension: exact count of live IDs above last_delivered_id.
    pub unread_entries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerInfo {
    pub name: Vec<u8>,
    pub pending: usize,
    pub idle_ms: u64,
    /// None means no new acknowledged-mode delivery or successful claim yet.
    /// Following Redis 8.2, NOACK and history reads do not refresh this clock.
    pub inactive_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullConsumerInfo {
    pub info: ConsumerInfo,
    pub pending: Vec<PendingInfo>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullGroupInfo {
    pub info: GroupInfo,
    pub pending: Vec<PendingInfo>,
    pub consumers: Vec<FullConsumerInfo>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullStreamInfo {
    pub info: StreamInfo,
    pub entries: Vec<Entry>,
    pub groups: Vec<FullGroupInfo>,
}
