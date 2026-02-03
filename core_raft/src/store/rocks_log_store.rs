use crate::network::raft_rocksdb::TypeConfig;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
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
use rocksdb::ColumnFamily;
use rocksdb::DB;
use rocksdb::Direction;
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc};
use std::time::Instant;
use indexmap::IndexMap;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct RocksLogStore {
    db: Arc<DB>,
    cache: Arc<RwLock<IndexMap<u64, EntryOf<TypeConfig>>>>,
    _p: PhantomData<TypeConfig>,
}

impl RocksLogStore {
    pub fn new(db: Arc<DB>) -> Self {
        db.cf_handle("meta")
            .expect("column family `meta` not found");
        db.cf_handle("logs")
            .expect("column family `logs` not found");

        Self {
            db,
            cache: Default::default(),
            _p: Default::default(),
        }
    }


    /// Insert entry into cache and evict oldest until capacity 100.
    async fn cache_insert(&mut self, entry: EntryOf<TypeConfig>) {
        let idx = entry.index();
        let mut cache =self.cache.write().await;
        // If the key already exists, replace the value (keep order).
        cache.insert(idx, entry);
        // Evict oldest while size > 100
        while cache.len() > 100 {
            // remove first inserted (oldest)
            cache.shift_remove_index(0);
        }
    }

    /// Try to satisfy the given range from cache.
    ///
    /// Returns Some(vec) if fully hit in cache; None if any element missing or end unbounded.
    async fn try_get_from_cache<RB: RangeBounds<u64> + Clone + Debug>(
        &self,
        range: RB,
    ) -> Option<Vec<EntryOf<TypeConfig>>> {
        // compute start index
        use std::ops::Bound;
        let start: u64 = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => x.saturating_add(1),
            Bound::Unbounded => 0,
        };
        // compute end index; if unbounded -> cannot satisfy from cache reliably
        let end_opt: Option<u64> = match range.end_bound() {
            Bound::Included(x) => Some(*x),
            Bound::Excluded(x) => {
                if *x == 0 {
                    return Some(Vec::new()); // empty range
                } else {
                    Some(x.saturating_sub(1))
                }
            }
            Bound::Unbounded => None,
        };

        let end = match end_opt {
            None => return None, // unbounded end -> fall back to rocksdb
            Some(e) => e,
        };

        if start > end {
            return Some(Vec::new()); // empty range
        }

        // quick check: if requested length is larger than cache capacity -> miss
        let len_needed = end.saturating_sub(start).saturating_add(1);
        if len_needed as usize > 100 {
            return None;
        }

        let mut out = Vec::with_capacity(len_needed as usize);
        let cache = self.cache.read().await;
        for idx in start..=end {
            match cache.get(&idx) {
                Some(ent) => out.push(ent.clone()),
                None => return None,
            }
        }
        Some(out)
    }


    fn cf_meta(&self) -> &ColumnFamily {
        self.db.cf_handle("meta").unwrap()
    }

    fn cf_logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    /// Get a store metadata.
    ///
    /// It returns `None` if the store does not have such a metadata stored.
    fn get_meta<M: StoreMeta<TypeConfig>>(&self) -> Result<Option<M::Value>, io::Error> {
        let bytes = self
            .db
            .get_cf(self.cf_meta(), M::KEY)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let t = bincode2::deserialize(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(t))
    }

    /// Save a store metadata.
    fn put_meta<M: StoreMeta<TypeConfig>>(&self, value: &M::Value) -> Result<(), io::Error> {
        let bin_value = bincode2::serialize(value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.db
            .put_cf(self.cf_meta(), M::KEY, bin_value)
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RocksLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<<TypeConfig as RaftTypeConfig>::Entry>, io::Error> {
        if let Some(cached) = self.try_get_from_cache(range.clone()).await {
            tracing::debug!("cache hit for range");
            return Ok(cached);
        }
        tracing::warn!("start={:?},end ={:?}",range.start_bound(),range.end_bound());
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        let mut res = Vec::new();
        let it = self.db.iterator_cf(
            self.cf_logs(),
            rocksdb::IteratorMode::From(&start, Direction::Forward),
        );
        for item_res in it {
            let (id, val) = item_res.map_err(read_logs_err)?;
            let id = bin_to_id(&id);
            if !range.contains(&id) {
                break;
            }
            let entry: EntryOf<TypeConfig> = bincode2::deserialize(&val).map_err(read_logs_err)?;
            assert_eq!(id, entry.index());
            res.push(entry);
        }
        Ok(res)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last = self
            .db
            .iterator_cf(self.cf_logs(), rocksdb::IteratorMode::End)
            .next();

        let last_log_id = match last {
            None => None,
            Some(res) => {
                let (_log_index, entry_bytes) = res.map_err(read_logs_err)?;
                let ent = bincode2::deserialize::<EntryOf<TypeConfig>>(&entry_bytes)
                    .map_err(read_logs_err)?;
                Some(ent.log_id())
            }
        };

        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;

        let last_log_id = match last_log_id {
            None => last_purged_log_id.clone(),
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
        let db = self.db.clone();
        TypeConfig::spawn_blocking(move || {
            db.flush_wal(true)
                .map_err(|e| io::Error::other(e.to_string()))
        })
            .await??;
        Ok(())
    }

    //心跳不会走到这里
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
    {
        let start = Instant::now();
        for entry in entries {
            let id = id_to_bin(entry.index());
            self.db
                .put_cf(
                    self.cf_logs(),
                    id,
                    bincode2::serialize(&entry)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                )
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.cache_insert(entry);
        }
        // 在调用回调函数之前，确保日志已经持久化到磁盘。
        //
        // 但上面的 `pub_cf()` 必须在这个函数中调用，而不能放到另一个任务里。
        // 因为当函数返回时，需要能够读取到这些日志条目。
        let db = self.db.clone();
        std::thread::spawn(move || {
            let res = db.flush_wal(true).map_err(io::Error::other);
            callback.io_completed(res);
            let elapsed = start.elapsed();
            tracing::info!("rocksdb append elapsed: {:?}", elapsed);
        });

        // Return now, and the callback will be invoked later when IO is done.
        Ok(())
    }

    // 如果follower的日志与leader的日志不匹配，follower会删除冲突的日志
    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        tracing::info!("truncate_after: ({:?}, +oo)", last_log_id);

        let start_index = match last_log_id {
            Some(log_id) => log_id.index() + 1,
            None => 0,
        };

        let from = id_to_bin(start_index);
        let to = id_to_bin(u64::MAX);
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| io::Error::other(e.to_string()))?;
        // Truncating does not need to be persisted.
        Ok(())
    }

    //日志压缩
    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        tracing::info!("delete_log: [0, {:?}]", log_id);

        // Write the last-purged log id before purging the logs.
        // The logs at and before last-purged log id will be ignored by openraft.
        // Therefore, there is no need to do it in a transaction.
        self.put_meta::<meta::LastPurged>(&log_id)?;

        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index() + 1);
        //删除指定范围内的所有数据
        self.db
            .delete_range_cf(self.cf_logs(), &from, &to)
            .map_err(|e| io::Error::other(e.to_string()))?;

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

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

fn read_logs_err(e: impl Error + 'static) -> io::Error {
    io::Error::other(e.to_string())
}
