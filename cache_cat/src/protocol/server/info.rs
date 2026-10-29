//! INFO command implementation.
//!
//! INFO is a local observability command.  It does not perform a lease read
//! or append a Raft log when executed on its own.  A transaction carries an
//! `InfoParams` marker through Raft so that keyspace information is sampled at
//! the marker's position in the transaction instead of when MULTI queues it.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::server::memory::stats::MemoryStats;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

/// INFO sections supported by cache-cat.  The three statistics sections are
/// recognized for Redis-compatible argument handling but currently emit no
/// fields because their counters have not been implemented yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InfoSection {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cpu,
    Modules,
    ErrorStats,
    Cluster,
    Keyspace,
    CommandStats,
    LatencyStats,
    Raft,
}

const SECTION_ORDER: [InfoSection; 14] = [
    InfoSection::Server,
    InfoSection::Clients,
    InfoSection::Memory,
    InfoSection::Persistence,
    InfoSection::Stats,
    InfoSection::Replication,
    InfoSection::Cpu,
    InfoSection::Modules,
    InfoSection::ErrorStats,
    InfoSection::Cluster,
    InfoSection::Keyspace,
    InfoSection::CommandStats,
    InfoSection::LatencyStats,
    InfoSection::Raft,
];

const DEFAULT_SECTIONS: [InfoSection; 11] = [
    InfoSection::Server,
    InfoSection::Clients,
    InfoSection::Memory,
    InfoSection::Persistence,
    InfoSection::Stats,
    InfoSection::Replication,
    InfoSection::Cpu,
    InfoSection::Modules,
    InfoSection::ErrorStats,
    InfoSection::Cluster,
    InfoSection::Keyspace,
];

impl InfoSection {
    fn parse(name: &[u8]) -> Option<Self> {
        if name.eq_ignore_ascii_case(b"server") {
            Some(Self::Server)
        } else if name.eq_ignore_ascii_case(b"clients") {
            Some(Self::Clients)
        } else if name.eq_ignore_ascii_case(b"memory") {
            Some(Self::Memory)
        } else if name.eq_ignore_ascii_case(b"persistence") {
            Some(Self::Persistence)
        } else if name.eq_ignore_ascii_case(b"stats") {
            Some(Self::Stats)
        } else if name.eq_ignore_ascii_case(b"replication") {
            Some(Self::Replication)
        } else if name.eq_ignore_ascii_case(b"cpu") {
            Some(Self::Cpu)
        } else if name.eq_ignore_ascii_case(b"modules") {
            Some(Self::Modules)
        } else if name.eq_ignore_ascii_case(b"errorstats") {
            Some(Self::ErrorStats)
        } else if name.eq_ignore_ascii_case(b"cluster") {
            Some(Self::Cluster)
        } else if name.eq_ignore_ascii_case(b"keyspace") {
            Some(Self::Keyspace)
        } else if name.eq_ignore_ascii_case(b"commandstats") {
            Some(Self::CommandStats)
        } else if name.eq_ignore_ascii_case(b"latencystats") {
            Some(Self::LatencyStats)
        } else if name.eq_ignore_ascii_case(b"raft") {
            Some(Self::Raft)
        } else {
            None
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Server => "Server",
            Self::Clients => "Clients",
            Self::Memory => "Memory",
            Self::Persistence => "Persistence",
            Self::Stats => "Stats",
            Self::Replication => "Replication",
            Self::Cpu => "CPU",
            Self::Modules => "Modules",
            Self::ErrorStats => "Errorstats",
            Self::Cluster => "Cluster",
            Self::Keyspace => "Keyspace",
            Self::CommandStats => "Commandstats",
            Self::LatencyStats => "Latencystats",
            Self::Raft => "Raft",
        }
    }
}

/// Runtime information captured before a transaction is submitted.
///
/// Keyspace is deliberately excluded: it is rendered from the state machine
/// while the transaction is being applied, after preceding operations have
/// taken effect.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InfoContext {
    sections: Vec<(InfoSection, String)>,
}

/// Serializable INFO request used by MULTI/EXEC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoParams {
    pub(crate) sections: Vec<InfoSection>,
    pub(crate) context: Option<InfoContext>,
}

impl InfoParams {
    pub fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.is_empty() {
            return Err(ProtocolError::WrongArgCount("info"));
        }

        let mut sections = Vec::new();
        if items.len() == 1 {
            sections.extend(DEFAULT_SECTIONS);
        } else {
            for item in &items[1..] {
                let name = item
                    .string_bytes_clone()
                    .ok_or(ProtocolError::InvalidArgument("section"))?;
                if name.eq_ignore_ascii_case(b"default") {
                    add_sections(&mut sections, DEFAULT_SECTIONS.iter().copied());
                } else if name.eq_ignore_ascii_case(b"all")
                    || name.eq_ignore_ascii_case(b"everything")
                {
                    add_sections(&mut sections, SECTION_ORDER.into_iter());
                } else if let Some(section) = InfoSection::parse(&name) {
                    if !sections.contains(&section) {
                        sections.push(section);
                    }
                }
                // Redis ignores unknown section names.  This also lets an
                // unsupported-only request return an empty bulk string.
            }
        }

        Ok(Self {
            sections,
            context: None,
        })
    }

    pub async fn execute_direct(&self, server: &RedisServer) -> Value {
        let context = Self::collect_context(&self.sections, server).await;
        let _read_lock = server.app.state_machine.data.kvs.read_lock.read();
        self.render(&server.app.state_machine.data.kvs, &context)
    }

    /// Capture fields that cannot be reconstructed by the Raft state machine.
    pub async fn collect_context(sections: &[InfoSection], server: &RedisServer) -> InfoContext {
        let want = |section| sections.contains(&section);
        let mut context = InfoContext::default();

        if want(InfoSection::Server) {
            let uptime = server.started_at.elapsed().as_secs();
            let mut body = String::new();
            line(&mut body, "redis_version", env!("CARGO_PKG_VERSION"));
            line(&mut body, "redis_mode", "standalone");
            line(&mut body, "os", std::env::consts::OS);
            line(&mut body, "arch_bits", usize::BITS);
            line(&mut body, "process_id", std::process::id());
            line(&mut body, "tcp_port", server.app.config.redis_port);
            line(&mut body, "uptime_in_seconds", uptime);
            line(&mut body, "uptime_in_days", uptime / 86_400);
            context.sections.push((InfoSection::Server, body));
        }

        if want(InfoSection::Clients) {
            let mut body = String::new();
            line(
                &mut body,
                "connected_clients",
                server
                    .connected_clients
                    .load(std::sync::atomic::Ordering::Relaxed),
            );
            line(&mut body, "blocked_clients", 0);
            context.sections.push((InfoSection::Clients, body));
        }

        if want(InfoSection::Memory) {
            if let Ok(stats) = MemoryStats::collect() {
                let mut body = String::new();
                line(&mut body, "used_memory_rss", stats.process_rss);
                line(&mut body, "used_memory_peak_rss", stats.process_peak_rss);
                line(&mut body, "allocator_committed", stats.committed);
                line(&mut body, "allocator_reserved", stats.reserved);
                context.sections.push((InfoSection::Memory, body));
            }
        }

        if want(InfoSection::Persistence) {
            let snapshot_in_progress = server
                .app
                .state_machine
                .data
                .raft_meta_data
                .lock()
                .await
                .snapshot_state();
            let mut body = String::new();
            line(
                &mut body,
                "rdb_bgsave_in_progress",
                usize::from(snapshot_in_progress),
            );
            line(&mut body, "aof_enabled", 0);
            context.sections.push((InfoSection::Persistence, body));
        }

        if want(InfoSection::Replication) {
            let nodes = server.app.cluster.nodes();
            let is_leader = server.app.cluster.is_leader();
            let leader = server.app.cluster.leader_addr().await;
            let mut body = String::new();
            line(
                &mut body,
                "role",
                if is_leader { "master" } else { "slave" },
            );
            line(&mut body, "connected_slaves", nodes.len().saturating_sub(1));
            if !is_leader {
                if let Some(endpoint) = leader {
                    line(&mut body, "master_host", endpoint.addr());
                    line(&mut body, "master_port", endpoint.redis_port());
                    line(&mut body, "master_link_status", "up");
                } else {
                    line(&mut body, "master_link_status", "down");
                }
            }
            context.sections.push((InfoSection::Replication, body));
        }

        if want(InfoSection::Modules) {
            let mut body = String::new();
            line(&mut body, "module_count", 0);
            context.sections.push((InfoSection::Modules, body));
        }

        if want(InfoSection::Cluster) {
            let mut body = String::new();
            line(&mut body, "cluster_enabled", 0);
            context.sections.push((InfoSection::Cluster, body));
        }

        if want(InfoSection::Raft) {
            let metadata = server.app.state_machine.data.raft_meta_data.lock().await;
            let mut body = String::new();
            line(&mut body, "node_id", server.app.cluster.node_id());
            line(
                &mut body,
                "is_leader",
                usize::from(server.app.cluster.is_leader()),
            );
            line(&mut body, "members", server.app.cluster.nodes().len());
            line(
                &mut body,
                "snapshot_in_progress",
                usize::from(metadata.snapshot_state()),
            );
            if let Some(log_id) = metadata.last_applied_log_id {
                line(&mut body, "last_applied_log_index", log_id.index);
            }
            context.sections.push((InfoSection::Raft, body));
        }

        context
    }

    pub fn render_from_cache(&self, cache: &MyCache) -> Value {
        let context = self.context.as_ref().cloned().unwrap_or_default();
        self.render(cache, &context)
    }

    fn render(&self, cache: &MyCache, context: &InfoContext) -> Value {
        let mut output = String::new();
        for section in SECTION_ORDER {
            if !self.sections.contains(&section) {
                continue;
            }

            let body = if section == InfoSection::Keyspace {
                Some(keyspace_body(cache))
            } else {
                context
                    .sections
                    .iter()
                    .find(|(candidate, _)| *candidate == section)
                    .map(|(_, body)| body.clone())
            };

            let Some(body) = body else { continue };
            if section != InfoSection::Keyspace && body.is_empty() {
                continue;
            }
            if !output.is_empty() {
                output.push_str("\r\n");
            }
            let _ = write!(output, "# {}", section.label());
            output.push_str("\r\n");
            output.push_str(&body);
        }

        Value::VerbatimString {
            format: "txt".to_owned(),
            data: Bytes::from(output),
        }
    }
}

fn add_sections<I>(sections: &mut Vec<InfoSection>, values: I)
where
    I: IntoIterator<Item = InfoSection>,
{
    for section in values {
        if !sections.contains(&section) {
            sections.push(section);
        }
    }
}

fn keyspace_body(cache: &MyCache) -> String {
    let mut body = String::new();
    for (db, database) in cache.databases.iter().enumerate() {
        let keys = database.mocha.len();
        if keys != 0 {
            let _ = write!(body, "db{db}:keys={keys}");
            body.push_str("\r\n");
        }
    }
    body
}

fn line<T: std::fmt::Display>(body: &mut String, key: &str, value: T) {
    let _ = write!(body, "{key}:{value}");
    body.push_str("\r\n");
}

pub struct InfoCommand;

#[async_trait]
impl Command for InfoCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = InfoParams::parse(items)?;
        if let Some(queue) = client.transaction_queue.as_mut() {
            use crate::raft::types::entry::request::{Operation, RedisOperation};
            queue.push(Operation::Redis(RedisOperation::RedisInfo(params)));
            return Ok(Value::queued());
        }
        Ok(params.execute_direct(server).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn bulk(value: &[u8]) -> Value {
        Value::BulkString(Some(Bytes::copy_from_slice(value)))
    }

    #[test]
    fn parses_default_and_case_insensitive_sections() {
        let params = InfoParams::parse(&[
            bulk(b"INFO"),
            bulk(b"SeRvEr"),
            bulk(b"KEYSPACE"),
            bulk(b"stats"),
            bulk(b"unknown"),
        ])
        .unwrap();

        assert_eq!(
            params.sections,
            vec![
                InfoSection::Server,
                InfoSection::Keyspace,
                InfoSection::Stats
            ]
        );
    }

    #[test]
    fn unsupported_statistics_sections_return_an_empty_text_reply() {
        let params = InfoParams::parse(&[
            bulk(b"INFO"),
            bulk(b"stats"),
            bulk(b"commandstats"),
            bulk(b"latencystats"),
        ])
        .unwrap();
        let cache = MyCache::new(1).unwrap();
        let value = params.render_from_cache(&cache);

        assert!(matches!(
            value,
            Value::VerbatimString { format, data }
                if format == "txt" && data.is_empty()
        ));
    }

    #[test]
    fn info_uses_redis_text_reply_encoding_for_resp2_and_resp3() {
        let cache = MyCache::new(1).unwrap();
        let params = InfoParams::parse(&[bulk(b"INFO"), bulk(b"server")]).unwrap();
        let value = params.render_from_cache(&cache);

        assert_eq!(value.encode()[0], b'$');
        assert_eq!(value.encode_proto(3)[0], b'=');
    }

    #[test]
    fn keyspace_is_rendered_from_the_current_cache() {
        let cache = MyCache::new(1).unwrap();
        cache.databases[0].mocha.insert_persistent(
            Bytes::from_static(b"key"),
            crate::raft::types::core::mocha::core::MyValue::new(
                crate::raft::types::core::value_object::ValueObject::String(Bytes::from_static(
                    b"value",
                )),
            ),
        );

        let params = InfoParams::parse(&[bulk(b"INFO"), bulk(b"keyspace")]).unwrap();
        let encoded = params.render_from_cache(&cache).encode();
        assert!(
            encoded
                .windows(b"# Keyspace\r\n".len())
                .any(|window| { window == b"# Keyspace\r\n" })
        );
        assert!(
            encoded
                .windows(b"db0:keys=1\r\n".len())
                .any(|window| { window == b"db0:keys=1\r\n" })
        );
    }
}
