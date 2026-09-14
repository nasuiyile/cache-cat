use crate::raft::store::raft_engine::MessageExtTyped;
use crate::raft::types::raft_types::{Entry, GroupId, TypeConfig};
use meta::StoreMeta;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use openraft::type_config::TypeConfigExt;
use raft_engine::{Engine, LogBatch};
use std::fmt::{Debug, Formatter};
use std::io;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use tracing::Instrument;

#[derive(Clone)]
pub struct LogStore {
    _p: PhantomData<TypeConfig>,
    engine: Arc<Engine>,
    group_id: GroupId,
}
impl Debug for LogStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksLogStore").finish()
    }
}

impl LogStore {
    pub fn new(group_id: GroupId, engine: Arc<Engine>) -> Self {
        // 明确指定类型
        Self {
            _p: Default::default(),
            engine,
            group_id,
        }
    }

    /// Get a store metadata.
    ///
    /// It returns `None` if the store does not have such a metadata stored.
    fn get_meta<M: StoreMeta<TypeConfig>>(&self) -> Result<Option<M::Value>, io::Error> {
        let key = M::KEY.as_bytes();
        let bytes = self
            .engine
            .get_message::<M::Value>(self.group_id as u64, key)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let res = match bytes {
            None => return Ok(None),
            Some(bytes) => bytes,
        };

        Ok(Some(res))
    }

    /// Save a store metadata.
    fn put_meta<M: StoreMeta<TypeConfig>>(&self, value: &M::Value) -> Result<(), io::Error> {
        let mut batch = LogBatch::with_capacity(256);
        batch
            .put_message(self.group_id as u64, M::KEY.as_bytes().to_vec(), value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.engine
            .write(&mut batch, false)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as RaftTypeConfig>::Entry>, io::Error> {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1, // 排除转换为包含
            Bound::Unbounded => 0,        // 从0开始
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1, // 包含转换为不包含
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX, // 到最大值
        };

        let mut res = Vec::new();

        // openraft tolerates (and expects) a short read at both ends of the
        // range: entries below `first_index` were removed by `purge`, entries
        // above `last_index` are not appended yet. raft-engine, however,
        // returns `EntryCompacted` / `EntryNotFound` for such ranges, and a
        // storage error here is fatal for the raft core (the replication
        // task reports it as `StorageError`). Clamp to what the engine holds.
        // A purge can still land between the clamp and the fetch, so retry a
        // few times before giving up.
        const ATTEMPTS: usize = 3;
        for attempt in 1..=ATTEMPTS {
            let group = self.group_id as u64;
            let (Some(first), Some(last)) = (self.engine.first_index(group), self.engine.last_index(group))
            else {
                return Ok(res);
            };
            let clamped_start = start.max(first);
            let clamped_end = end.min(last + 1);
            if clamped_start >= clamped_end {
                return Ok(res);
            }
            match self.engine.fetch_entries_to::<MessageExtTyped>(
                group,
                clamped_start,
                clamped_end,
                None,
                &mut res,
            ) {
                Ok(_) => return Ok(res),
                Err(raft_engine::Error::EntryCompacted) if attempt < ATTEMPTS => {
                    res.clear();
                    continue;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    //不会在每次提交条目时被调用，但重启等场景会调用
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_log_id = match self.engine.last_index(self.group_id as u64) {
            None => None, //  只要 last_index 为 None，直接返回 None
            Some(i) => self
                .engine
                .get_entry::<MessageExtTyped>(self.group_id as u64, i)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .map(|entry| entry.log_id()),
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;
        let last_log_id = match last_log_id {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        self.put_meta::<meta::Vote>(vote)?;
        // Vote must be persisted to disk before returning.
        let engine = self.engine.clone();
        TypeConfig::spawn_blocking(move || {
            engine.sync().map_err(|e| io::Error::other(e.to_string()))
        })
        .await??;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
    {
        let mut batch = LogBatch::with_capacity(256);
        let x: Vec<Entry> = entries.into_iter().collect();
        batch
            .add_entries::<MessageExtTyped>(self.group_id as u64, &x)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        //提前释放
        // 在调用回调函数之前，确保日志已经持久化到磁盘。
        //
        // 但上面的 `pub_cf()` 必须在这个函数中调用，而不能放到另一个任务里。
        // 因为当函数返回时，需要能够读取到这些日志条目。
        // let db = self.db.clone();
        self.engine
            .write(&mut batch, false)
            .map_err(io::Error::other)?;

        let engine = self.engine.clone();
        let _hand = tokio::task::spawn_blocking(move || {
            let res = engine.sync().map_err(io::Error::other);
            callback.io_completed(res);
        })
        .instrument(tracing::debug_span!("raft-engine-sync"));
        // Return now, and the callback will be invoked later when IO is done.
        Ok(())
    }

    // 如果follower的日志与leader的日志不匹配，follower会删除冲突的日志
    //
    // raft-engine has no "drop the tail" primitive: it only knows `Compact`
    // (drop a prefix) and overwrite-on-append (appending at an index that is
    // already present drops everything from that index on). Leaving this a
    // no-op is fine as long as the conflicting entries get overwritten by the
    // very next append, which is what happens on the normal log-matching
    // path. It is *not* fine on the two paths where openraft truncates without
    // appending afterwards:
    //
    // - `install_full_snapshot` truncates to `committed` and then purges up to
    //   the snapshot's last log id; a stale tail (from an older leader) beyond
    //   that index stays on disk, and after a restart `get_log_state` reports
    //   `last_log_id < last_purged_log_id` (older term), which openraft treats
    //   as a corrupted store and refuses to start.
    // - a `prev_log_id` mismatch truncates and replies "conflict" without
    //   appending anything.
    //
    // So implement it with the primitives raft-engine does have: re-append the
    // boundary entry (same content) so the engine drops the tail behind it, or
    // compact everything away when the boundary is not in the engine any more.
    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        tracing::debug!("truncate_after: ({:?}, +oo)", last_log_id);
        let group = self.group_id as u64;
        let (Some(first), Some(last)) = (self.engine.first_index(group), self.engine.last_index(group))
        else {
            // Nothing stored, nothing to truncate.
            return Ok(());
        };

        let keep_upto = last_log_id.as_ref().map(|id| id.index);
        if keep_upto.is_some_and(|idx| idx >= last) {
            // Nothing after `last_log_id`.
            return Ok(());
        }

        match keep_upto {
            Some(idx) if idx >= first => {
                let boundary = self
                    .engine
                    .get_entry::<MessageExtTyped>(group, idx)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("log entry {} is within [{}, {}] but missing", idx, first, last),
                        )
                    })?;
                let mut batch = LogBatch::with_capacity(1);
                // 读取后原样写回， raft-engine 会丢弃 `(idx, last]` 范围内的日志 实现删除冲突日志的效果。
                batch
                    .add_entries::<MessageExtTyped>(group, &[boundary])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                self.engine.write(&mut batch, false).map_err(io::Error::other)?;
            }
            _ => {
                // Either everything must go (`None`) or the boundary was already
                // purged: every stored entry is behind it, drop them all. The
                // memtable becomes empty, and raft-engine accepts an append at
                // any index on an empty memtable, so a later append at
                // `last_log_id + 1` (or at 1) works.
                self.engine.compact_to(group, last + 1);
            }
        }

        // A snapshot installed right after this truncation is fsync'ed; if the
        // truncation were not, a crash could resurrect the stale tail next to
        // the new snapshot and fail openraft's startup consistency check.
        // Truncation is rare (conflicts only), so the extra fsync is cheap.
        let engine = self.engine.clone();
        TypeConfig::spawn_blocking(move || {
            engine.sync().map_err(|e| io::Error::other(e.to_string()))
        })
        .await??;
        Ok(())
    }

    //日志压缩
    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        // 在清理日志前记录最后清理的日志ID。
        // openraft 将忽略最后清理日志ID及之前的所有日志。
        // 因此，无需在事务中执行此操作
        self.put_meta::<meta::LastPurged>(&log_id)?;

        self.engine
            .compact_to(self.group_id as u64, log_id.index + 1);

        // Purging does not need to be persistent.
        Ok(())
    }
}

/// Metadata of a raft-store.
///
/// In raft, except logs and state machine, the store also has to store several piece of metadata.
/// This sub mod defines the key-value pairs of these metadata.
mod meta {
    use openraft::RaftTypeConfig;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;

    /// Defines metadata key and value
    pub(crate) trait StoreMeta<C>
    where
        C: RaftTypeConfig,
    {
        /// The key used to store in rocksdb
        const KEY: &'static str;

        /// The type of the value to store
        type Value: serde::Serialize + serde::de::DeserializeOwned;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl<C> StoreMeta<C> for LastPurged
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogIdOf<C>;
    }
    impl<C> StoreMeta<C> for Vote
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::store::raft_engine::create_raft_engine;
    use crate::raft::types::raft_types::{LeaderId, LogId};

    fn log_id(term: u64, index: u64) -> LogId {
        LogId::new(LeaderId { term, node_id: 1 }, index)
    }

    fn blank(term: u64, index: u64) -> Entry {
        Entry::new_blank(log_id(term, index))
    }

    async fn append(store: &mut LogStore, entries: Vec<Entry>) {
        store.append(entries, IOFlushed::noop()).await.unwrap();
    }

    async fn indexes(store: &mut LogStore) -> Vec<u64> {
        store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.log_id().index)
            .collect()
    }

    fn new_store() -> (tempfile::TempDir, LogStore) {
        let dir = tempfile::tempdir().unwrap();
        let engine = create_raft_engine(dir.path().join("raft-engine")).unwrap();
        (dir, LogStore::new(0, engine))
    }

    #[tokio::test]
    async fn truncate_after_drops_the_tail_and_allows_reappend() {
        let (_dir, mut store) = new_store();
        append(&mut store, (1..=5).map(|i| blank(1, i)).collect()).await;
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5]);

        // Conflict at 4: the leader's log continues with a different entry.
        store.truncate_after(Some(log_id(1, 3))).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3]);
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(1, 3))
        );

        // The leader's version of 4.. is appended afterwards.
        append(&mut store, vec![blank(2, 4), blank(2, 5), blank(2, 6)]).await;
        let entries = store.try_get_log_entries(4..).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id()).collect::<Vec<_>>(),
            vec![log_id(2, 4), log_id(2, 5), log_id(2, 6)]
        );

        // Truncating at (or beyond) the end is a no-op.
        store.truncate_after(Some(log_id(2, 6))).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn truncate_after_none_and_after_purged_boundary_empty_the_log() {
        let (_dir, mut store) = new_store();
        append(&mut store, (1..=4).map(|i| blank(1, i)).collect()).await;

        store.truncate_after(None).await.unwrap();
        assert!(indexes(&mut store).await.is_empty());
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);

        // The log can be rebuilt from index 1 afterwards.
        append(&mut store, (1..=6).map(|i| blank(2, i)).collect()).await;
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5, 6]);

        // install_full_snapshot pattern: purge a prefix, then truncate to a
        // boundary that is no longer in the engine -> the (stale) rest goes.
        store.purge(log_id(2, 3)).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![4, 5, 6]);
        store.truncate_after(Some(log_id(2, 3))).await.unwrap();
        assert!(indexes(&mut store).await.is_empty());
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(2, 3)));
        assert_eq!(state.last_log_id, Some(log_id(2, 3)));

        // ...and the snapshot's successor entries can be appended at 4.
        append(&mut store, vec![blank(3, 4)]).await;
        assert_eq!(indexes(&mut store).await, vec![4]);
    }

    #[tokio::test]
    async fn reads_below_the_purged_prefix_are_clamped_not_errors() {
        let (_dir, mut store) = new_store();
        append(&mut store, (1..=6).map(|i| blank(1, i)).collect()).await;
        store.purge(log_id(1, 3)).await.unwrap();
        // openraft tolerates a short read at the purged end of the range.
        assert_eq!(indexes(&mut store).await, vec![4, 5, 6]);
        let partial = store.try_get_log_entries(2..5).await.unwrap();
        assert_eq!(
            partial.iter().map(|e| e.log_id().index).collect::<Vec<_>>(),
            vec![4]
        );
    }
}
