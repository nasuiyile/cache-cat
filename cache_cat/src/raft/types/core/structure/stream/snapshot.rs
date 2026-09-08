//! Logical snapshot format. It restores stream-internal state without replaying
//! commands or exposing mutable internals outside the stream module.

use blart::TreeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde::ser::SerializeStruct;
use std::{collections::HashMap, fmt, sync::Arc};
use crate::raft::types::core::structure::stream::clock::{Clock, SystemClock};
use crate::raft::types::core::structure::stream::core::{
    Consumer, Group, Key, Pending, RedisStream,
};
use crate::raft::types::core::structure::stream::id::StreamId;
use crate::raft::types::core::structure::stream::types::{Entry, Fields};

pub const SNAPSHOT_VERSION: u32 = 1;

/// Owned, versioned logical snapshot. No runtime locks, notifications, identity
/// pointers, or clock implementations are persisted. Binary names remain bytes.
///
/// Deserializing this DTO does not validate it; `into_stream` does. Deserializing
/// RedisStream or SharedStream validates automatically. Bound input sizes at the
/// codec/I/O boundary when decoding untrusted data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamSnapshot {
    pub version: u32,
    pub last_generated_id: StreamId,
    pub entries_added: u64,
    pub max_deleted_entry_id: StreamId,
    pub entries: Vec<Entry>,
    pub groups: Vec<GroupSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupSnapshot {
    pub name: Vec<u8>,
    pub last_delivered: StreamId,
    pub entries_read: Option<u64>,
    pub consumers: Vec<ConsumerSnapshot>,
    /// The authoritative group PEL. Consumer PEL indexes are rebuilt from here.
    pub pending: Vec<PendingSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerSnapshot {
    pub name: Vec<u8>,
    pub seen_ms: u64,
    pub active_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingSnapshot {
    pub id: StreamId,
    pub consumer: Vec<u8>,
    /// Absolute Unix milliseconds, not a cached idle duration.
    pub delivery_ms: u64,
    pub deliveries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotError(String);

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid stream snapshot: {}", self.0)
    }
}
impl std::error::Error for SnapshotError {}

impl StreamSnapshot {
    /// Restore with the production wall clock. Time spent offline counts toward
    /// idle time. Use `into_stream_with_clock` for deterministic test/replay clocks.
    pub fn into_stream(self) -> Result<RedisStream, SnapshotError> {
        self.into_stream_with_clock(Arc::new(SystemClock))
    }

    pub fn into_stream_with_clock(
        self,
        clock: Arc<dyn Clock>,
    ) -> Result<RedisStream, SnapshotError> {
        if self.version != SNAPSHOT_VERSION {
            return Err(SnapshotError(format!("unsupported version {}", self.version)));
        }
        if u64::try_from(self.entries.len()).map_or(true, |n| n > self.entries_added) {
            return Err(SnapshotError("entries_added is below live length".into()));
        }
        if self.max_deleted_entry_id > self.last_generated_id {
            return Err(SnapshotError("deleted marker exceeds last-generated ID".into()));
        }

        let mut stream = RedisStream::with_clock(clock);
        stream.last_generated_id = self.last_generated_id;
        stream.entries_added = self.entries_added;
        stream.max_deleted_entry_id = self.max_deleted_entry_id;

        let mut previous = StreamId::ZERO;
        for entry in self.entries {
            if entry.id <= previous || entry.id > stream.last_generated_id {
                return Err(SnapshotError(
                    "live IDs must be nonzero, strictly increasing, and at most last-generated ID".into(),
                ));
            }
            if entry.fields.is_empty() {
                return Err(SnapshotError("entry has no field-value pairs".into()));
            }
            previous = entry.id;
            let _ = stream.entries.insert(entry.id.to_key(), entry.fields);
        }

        for saved in self.groups {
            if stream.groups.contains_key(saved.name.as_slice()) {
                return Err(SnapshotError("duplicate consumer group name".into()));
            }
            // Cursor and entries_read can exceed stream metadata after legal
            // administrative SETID operations. Do not clamp or recompute them.
            let mut group = Group::new(saved.last_delivered, saved.entries_read);
            for consumer in saved.consumers {
                if group.consumers.contains_key(consumer.name.as_slice()) {
                    return Err(SnapshotError("duplicate consumer name".into()));
                }
                group.consumers.insert(consumer.name, Consumer {
                    pel: TreeMap::new(),
                    seen_ms: consumer.seen_ms,
                    active_ms: consumer.active_ms,
                });
            }
            let mut previous = StreamId::ZERO;
            for pending in saved.pending {
                if pending.id <= previous {
                    return Err(SnapshotError(
                        "PEL IDs must be nonzero and strictly increasing".into(),
                    ));
                }
                if !group.consumers.contains_key(pending.consumer.as_slice()) {
                    return Err(SnapshotError("PEL owner is not a registered consumer".into()));
                }
                previous = pending.id;
                // Missing payload is valid: XDEL/KEEPREF leaves PEL tombstones.
                // Zero delivery counts and future timestamps are also legal.
                group.assign(pending.id.to_key(), Pending {
                    consumer: pending.consumer,
                    delivery_ms: pending.delivery_ms,
                    deliveries: pending.deliveries,
                });
            }
            stream.groups.insert(saved.name, group);
        }
        // All fallible checks occur before returning the new independent stream.
        stream.check_invariants().map_err(SnapshotError)?;
        Ok(stream)
    }
}

impl RedisStream {
    /// Copy logical state, not ART topology or consumer secondary PEL indexes.
    /// Encoding this snapshot later does not require a lock on the live stream.
    pub fn snapshot(&self) -> StreamSnapshot {
        let mut groups: Vec<_> = self.groups.iter().map(|(name, g)| {
            let mut consumers: Vec<_> = g.consumers.iter().map(|(name, c)| {
                ConsumerSnapshot {
                    name: name.clone(), seen_ms: c.seen_ms, active_ms: c.active_ms,
                }
            }).collect();
            consumers.sort_unstable_by(|a, b| a.name.cmp(&b.name));
            GroupSnapshot {
                name: name.clone(),
                last_delivered: g.last_delivered,
                entries_read: g.entries_read,
                consumers,
                pending: g.pel.iter().map(|(key, p)| PendingSnapshot {
                    id: StreamId::from_key(*key),
                    consumer: p.consumer.clone(),
                    delivery_ms: p.delivery_ms,
                    deliveries: p.deliveries,
                }).collect(),
            }
        }).collect();
        groups.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        StreamSnapshot {
            version: SNAPSHOT_VERSION,
            last_generated_id: self.last_generated_id,
            entries_added: self.entries_added,
            max_deleted_entry_id: self.max_deleted_entry_id,
            entries: self.entries.iter().map(|(key, fields)| Entry {
                id: StreamId::from_key(*key), fields: fields.clone(),
            }).collect(),
            groups,
        }
    }
}

// Borrowed views use exactly the same field order and Serde struct names as the
// owned DTO. This supports non-self-describing formats as well as JSON, without
// cloning payloads or building a complete intermediate snapshot during encoding.
#[derive(Serialize)]
#[serde(rename = "Entry")]
struct EntryRef<'a> {
    id: StreamId,
    fields: &'a Fields,
}
struct EntriesRef<'a>(&'a TreeMap<Key, Fields>);
impl Serialize for EntriesRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|(key, fields)| EntryRef {
            id: StreamId::from_key(*key), fields,
        }))
    }
}

#[derive(Serialize)]
#[serde(rename = "ConsumerSnapshot")]
struct ConsumerRef<'a> {
    name: &'a [u8],
    seen_ms: u64,
    active_ms: Option<u64>,
}
struct ConsumersRef<'a>(&'a HashMap<Vec<u8>, Consumer>);
impl Serialize for ConsumersRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut ordered: Vec<_> = self.0.iter().collect();
        ordered.sort_unstable_by(|a, b| a.0.cmp(b.0));
        serializer.collect_seq(ordered.into_iter().map(|(name, c)| ConsumerRef {
            name, seen_ms: c.seen_ms, active_ms: c.active_ms,
        }))
    }
}

#[derive(Serialize)]
#[serde(rename = "PendingSnapshot")]
struct PendingRef<'a> {
    id: StreamId,
    consumer: &'a [u8],
    delivery_ms: u64,
    deliveries: u64,
}
struct PendingListRef<'a>(&'a TreeMap<Key, Pending>);
impl Serialize for PendingListRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|(key, p)| PendingRef {
            id: StreamId::from_key(*key), consumer: &p.consumer,
            delivery_ms: p.delivery_ms, deliveries: p.deliveries,
        }))
    }
}

#[derive(Serialize)]
#[serde(rename = "GroupSnapshot")]
struct GroupRef<'a> {
    name: &'a [u8],
    last_delivered: StreamId,
    entries_read: Option<u64>,
    consumers: ConsumersRef<'a>,
    pending: PendingListRef<'a>,
}
struct GroupsRef<'a>(&'a HashMap<Vec<u8>, Group>);
impl Serialize for GroupsRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut ordered: Vec<_> = self.0.iter().collect();
        ordered.sort_unstable_by(|a, b| a.0.cmp(b.0));
        serializer.collect_seq(ordered.into_iter().map(|(name, g)| GroupRef {
            name,
            last_delivered: g.last_delivered,
            entries_read: g.entries_read,
            consumers: ConsumersRef(&g.consumers),
            pending: PendingListRef(&g.pel),
        }))
    }
}

impl Serialize for RedisStream {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("StreamSnapshot", 6)?;
        state.serialize_field("version", &SNAPSHOT_VERSION)?;
        state.serialize_field("last_generated_id", &self.last_generated_id)?;
        state.serialize_field("entries_added", &self.entries_added)?;
        state.serialize_field("max_deleted_entry_id", &self.max_deleted_entry_id)?;
        state.serialize_field("entries", &EntriesRef(&self.entries))?;
        state.serialize_field("groups", &GroupsRef(&self.groups))?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RedisStream {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        StreamSnapshot::deserialize(deserializer)?
            .into_stream()
            .map_err(serde::de::Error::custom)
    }
}
