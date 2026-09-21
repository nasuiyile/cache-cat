use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;

use async_trait::async_trait;
use bytes::Bytes;
use mimalloc::MiMalloc;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct MiStatCount {
    #[serde(default)]
    total: i64,

    #[serde(default)]
    peak: i64,

    #[serde(default)]
    current: i64,
}

#[derive(Debug, Deserialize)]
struct MiProcessStats {
    #[serde(default)]
    rss_current: u64,

    #[serde(default)]
    rss_peak: u64,

    #[serde(default)]
    commit_current: u64,

    #[serde(default)]
    commit_peak: u64,

    #[serde(default)]
    page_faults: u64,
}

#[derive(Debug, Deserialize)]
struct MiMallocStats {
    process: MiProcessStats,

    reserved: MiStatCount,
    committed: MiStatCount,

    #[serde(default)]
    mmap_calls: i64,

    #[serde(default)]
    commit_calls: i64,

    #[serde(default)]
    purge_calls: i64,

    #[serde(default)]
    purged: i64,
}

#[derive(Debug)]
struct MemoryStats {
    process_rss: u64,
    process_peak_rss: u64,

    process_commit: u64,
    process_peak_commit: u64,

    reserved: u64,
    committed: u64,

    page_faults: u64,

    mmap_calls: u64,
    commit_calls: u64,
    purge_calls: u64,
    purged_bytes: u64,
}

impl MemoryStats {
    fn collect() -> Result<Self, &'static str> {
        let json = MiMalloc::stats_json().map_err(|_| "failed to get mimalloc stats")?;

        let raw: MiMallocStats = serde_json::from_slice(json.to_bytes())
            .map_err(|_| "failed to parse mimalloc stats")?;

        Ok(Self {
            /*
             * OS / process 级别统计。
             *
             * 这些来自 mi_process_info()，比 malloc_normal /
             * malloc_huge 这种 thread-local allocator stats
             * 更适合多线程 server。
             */
            process_rss: raw.process.rss_current,
            process_peak_rss: raw.process.rss_peak,

            process_commit: raw.process.commit_current,
            process_peak_commit: raw.process.commit_peak,

            /*
             * allocator VM 状态。
             *
             * 注意：
             * committed != Redis allocator.resident
             * reserved  != RSS
             *
             * 所以保留 mimalloc 自己的命名，不冒充 Redis 字段。
             */
            reserved: non_negative(raw.reserved.current),
            committed: non_negative(raw.committed.current),

            page_faults: raw.process.page_faults,

            mmap_calls: non_negative(raw.mmap_calls),
            commit_calls: non_negative(raw.commit_calls),
            purge_calls: non_negative(raw.purge_calls),
            purged_bytes: non_negative(raw.purged),
        })
    }

    fn into_value(self) -> Value {
        let mut values = Vec::with_capacity(24);

        /*
         * Process memory
         */
        push_integer(&mut values, "process.rss", self.process_rss);

        push_integer(&mut values, "process.peak-rss", self.process_peak_rss);

        push_integer(&mut values, "process.commit", self.process_commit);

        push_integer(&mut values, "process.peak-commit", self.process_peak_commit);

        push_integer(&mut values, "process.page-faults", self.page_faults);

        /*
         * mimalloc memory
         */
        push_integer(&mut values, "mimalloc.reserved", self.reserved);

        push_integer(&mut values, "mimalloc.committed", self.committed);

        /*
         * mimalloc allocator operations
         */
        push_integer(&mut values, "mimalloc.mmap-calls", self.mmap_calls);

        push_integer(&mut values, "mimalloc.commit-calls", self.commit_calls);

        push_integer(&mut values, "mimalloc.purge-calls", self.purge_calls);

        push_integer(&mut values, "mimalloc.purged-bytes", self.purged_bytes);

        Value::Array(Some(values))
    }
}

fn non_negative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn push_key(values: &mut Vec<Value>, key: &'static str) {
    values.push(Value::BulkString(Some(Bytes::from_static(key.as_bytes()))));
}

fn push_integer(values: &mut Vec<Value>, key: &'static str, value: u64) {
    push_key(values, key);

    values.push(Value::Integer(i64::try_from(value).unwrap_or(i64::MAX)));
}

pub struct MemoryStatsCommand;

impl MemoryStatsCommand {
    fn parse(items: &[Value]) -> Result<(), ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("MEMORY STATS"));
        }

        let memory = items[0]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("command"))?;

        if !memory.as_ref().eq_ignore_ascii_case(b"MEMORY") {
            return Err(ProtocolError::InvalidArgument("command"));
        }

        let stats = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?;

        if !stats.as_ref().eq_ignore_ascii_case(b"STATS") {
            return Err(ProtocolError::InvalidArgument("subcommand"));
        }

        Ok(())
    }
}

#[async_trait]
impl SubCommand for MemoryStatsCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Self::parse(items)?;

        let stats =
            MemoryStats::collect().map_err(|_| ProtocolError::InvalidArgument("mimalloc stats"))?;

        Ok(stats.into_value())
    }
}
