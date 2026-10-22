//! INFO command implementation
//!
//! INFO [section [section ...]]
//!
//! Only sections and fields whose value can be observed on this node are
//! reported. Sections without a real data source are left out instead of
//! being filled with placeholder numbers.

use crate::error::CacheCatError;
use crate::protocol::command::{Client, Command};
use crate::protocol::server::memory::stats::mimalloc_stats;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::store::snapshot::snapshot_handler::get_snapshot_file_name;
use crate::raft::types::core::response_value::Value;
use crate::utils::times::{now_s, now_us};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::time::UNIX_EPOCH;

/// Clients gate feature usage on redis_version, so report a compatible Redis
/// release. The real product version is exposed as cache_cat_version.
const COMPAT_REDIS_VERSION: &str = "7.4.0";

/// Sections that INFO can render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Section {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cluster,
    Keyspace,
}

/// Sections returned without arguments and by `default`. No module or
/// command-level statistics exist, so `all` reuses this same set.
const DEFAULT_SECTIONS: [Section; 8] = [
    Section::Server,
    Section::Clients,
    Section::Memory,
    Section::Persistence,
    Section::Stats,
    Section::Replication,
    Section::Cluster,
    Section::Keyspace,
];

const SECTION_NAMES: [(&[u8], Section); 8] = [
    (b"server", Section::Server),
    (b"clients", Section::Clients),
    (b"memory", Section::Memory),
    (b"persistence", Section::Persistence),
    (b"stats", Section::Stats),
    (b"replication", Section::Replication),
    (b"cluster", Section::Cluster),
    (b"keyspace", Section::Keyspace),
];

impl Section {
    /// Match a section name, case-insensitively like Redis.
    fn parse(arg: &[u8]) -> Option<Self> {
        SECTION_NAMES
            .iter()
            .find(|&&(name, _)| arg.eq_ignore_ascii_case(name))
            .map(|(_, section)| *section)
    }

    /// Header line printed before the fields of this section.
    const fn header(self) -> &'static str {
        match self {
            Self::Server => "# Server\r\n",
            Self::Clients => "# Clients\r\n",
            Self::Memory => "# Memory\r\n",
            Self::Persistence => "# Persistence\r\n",
            Self::Stats => "# Stats\r\n",
            Self::Replication => "# Replication\r\n",
            Self::Cluster => "# Cluster\r\n",
            Self::Keyspace => "# Keyspace\r\n",
        }
    }
}

/// INFO command handler.
pub struct InfoCommand;

#[async_trait]
impl Command for InfoCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut text = String::new();
        for section in select_sections(items) {
            render_section(section, server, &mut text).await;
        }
        Ok(Value::VerbatimString {
            format: "txt".into(),
            data: Bytes::from(text),
        })
    }
}

/// Resolve requested sections in argument order, dropping duplicates. Unknown
/// names are ignored; with no name matching anything the reply is empty.
fn select_sections(items: &[Value]) -> Vec<Section> {
    let mut selected = Vec::new();
    if items.len() == 1 {
        selected.extend(DEFAULT_SECTIONS);
        return selected;
    }
    for item in items.iter().skip(1) {
        let Some(arg) = item.string_bytes_clone() else {
            continue;
        };
        if arg.eq_ignore_ascii_case(b"default")
            || arg.eq_ignore_ascii_case(b"all")
            || arg.eq_ignore_ascii_case(b"everything")
        {
            push_sections(&mut selected, &DEFAULT_SECTIONS);
        } else if let Some(section) = Section::parse(&arg) {
            push_sections(&mut selected, &[section]);
        }
    }
    selected
}

/// Append sections that are not present yet.
fn push_sections(selected: &mut Vec<Section>, sections: &[Section]) {
    for section in sections {
        if !selected.contains(section) {
            selected.push(*section);
        }
    }
}

/// Render one section followed by the empty line Redis emits between sections.
async fn render_section(section: Section, server: &RedisServer, out: &mut String) {
    out.push_str(section.header());
    match section {
        Section::Server => render_server(server, out),
        Section::Clients => render_clients(server, out).await,
        Section::Memory => render_memory(out),
        Section::Persistence => render_persistence(server, out).await,
        Section::Stats => render_stats(server, out).await,
        Section::Replication => render_replication(server, out).await,
        Section::Cluster => out.push_str("cluster_enabled:0\r\n"),
        Section::Keyspace => render_keyspace(server, out),
    }
    out.push_str("\r\n");
}

/// server section.
fn render_server(server: &RedisServer, out: &mut String) {
    out.push_str(&format!("redis_version:{COMPAT_REDIS_VERSION}\r\n"));
    out.push_str(&format!("cache_cat_version:{}\r\n", env!("CARGO_PKG_VERSION")));
    out.push_str("redis_git_sha1:00000000\r\n");
    out.push_str("redis_git_dirty:0\r\n");
    out.push_str("redis_mode:standalone\r\n");
    out.push_str(&format!(
        "os:{} {}\r\n",
        std::env::consts::OS,
        std::env::consts::ARCH
    ));
    out.push_str(&format!("arch_bits:{}\r\n", usize::BITS));
    out.push_str(&format!("process_id:{}\r\n", std::process::id()));
    out.push_str(&format!("run_id:{}\r\n", server.run_id));
    out.push_str(&format!("tcp_port:{}\r\n", server.app.config.redis_port));
    out.push_str(&format!("server_time_usec:{}\r\n", now_us()));
    let uptime = now_s().saturating_sub(server.start_time_ms / 1000);
    out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
    out.push_str(&format!("uptime_in_days:{}\r\n", uptime / 86400));
}

/// clients section.
async fn render_clients(server: &RedisServer, out: &mut String) {
    out.push_str(&format!(
        "connected_clients:{}\r\n",
        server.connected_clients.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "blocked_clients:{}\r\n",
        server.app.blocking_keys.waiter_count()
    ));
    out.push_str(&format!(
        "pubsub_clients:{}\r\n",
        server.broadcast.client_count().await
    ));
}

/// memory section. When allocator statistics are unavailable only the static
/// configuration fields are printed.
fn render_memory(out: &mut String) {
    if let Ok(raw) = mimalloc_stats() {
        let used_memory = raw.process.commit_current;
        let used_memory_rss = raw.process.rss_current;
        let used_memory_peak = raw.process.commit_peak;
        out.push_str(&format!("used_memory:{used_memory}\r\n"));
        out.push_str(&format!(
            "used_memory_human:{}\r\n",
            bytes_to_human(used_memory)
        ));
        out.push_str(&format!("used_memory_rss:{used_memory_rss}\r\n"));
        out.push_str(&format!(
            "used_memory_rss_human:{}\r\n",
            bytes_to_human(used_memory_rss)
        ));
        out.push_str(&format!("used_memory_peak:{used_memory_peak}\r\n"));
        out.push_str(&format!(
            "used_memory_peak_human:{}\r\n",
            bytes_to_human(used_memory_peak)
        ));
    }
    out.push_str("maxmemory:0\r\n");
    out.push_str("maxmemory_human:0B\r\n");
    out.push_str("maxmemory_policy:noeviction\r\n");
    out.push_str("mem_allocator:mimalloc\r\n");
}

/// persistence section.
async fn render_persistence(server: &RedisServer, out: &mut String) {
    out.push_str("loading:0\r\n");
    let snapshotting = server
        .app
        .state_machine
        .data
        .raft_meta_data
        .lock()
        .await
        .snapshot_state();
    out.push_str(&format!(
        "rdb_bgsave_in_progress:{}\r\n",
        u8::from(snapshotting)
    ));
    out.push_str(&format!(
        "rdb_last_save_time:{}\r\n",
        snapshot_modified_seconds(server).await
    ));
}

/// stats section.
async fn render_stats(server: &RedisServer, out: &mut String) {
    out.push_str(&format!(
        "total_connections_received:{}\r\n",
        server.total_connections_received.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "total_commands_processed:{}\r\n",
        server.total_commands_processed.load(Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "pubsub_channels:{}\r\n",
        server.broadcast.channel_count().await
    ));
    out.push_str(&format!(
        "pubsub_patterns:{}\r\n",
        server.broadcast.pattern_count().await
    ));
}

/// replication section.
async fn render_replication(server: &RedisServer, out: &mut String) {
    let role = if server.app.cluster.is_leader() {
        "master"
    } else {
        "slave"
    };
    out.push_str(&format!("role:{role}\r\n"));
    if role == "slave" {
        match server.app.cluster.leader_addr().await {
            Some(leader) => {
                out.push_str(&format!("master_host:{}\r\n", leader.addr()));
                out.push_str(&format!("master_port:{}\r\n", leader.redis_port()));
                out.push_str("master_link_status:up\r\n");
            }
            None => out.push_str("master_link_status:down\r\n"),
        }
        out.push_str("slave_read_only:1\r\n");
    }
    let replicas = server.app.cluster.replicas().await;
    out.push_str(&format!(
        "connected_slaves:{}\r\n",
        replicas.iter().filter(|(_, _, alive, _)| *alive).count()
    ));
    for (index, (_, node, alive, lag_ms)) in replicas.iter().enumerate() {
        out.push_str(&format!(
            "slave{index}:ip={},port={},state={},lag={}\r\n",
            node.endpoint.addr(),
            node.endpoint.redis_port(),
            if *alive { "online" } else { "offline" },
            lag_ms / 1000
        ));
    }
}

/// keyspace section, listing non-empty databases only.
fn render_keyspace(server: &RedisServer, out: &mut String) {
    for (index, database) in server.app.state_machine.data.kvs.databases.iter().enumerate() {
        let keys = database.mocha.len();
        if keys == 0 {
            continue;
        }
        out.push_str(&format!(
            "db{index}:keys={keys},expires={}\r\n",
            database.mocha.expire_count()
        ));
    }
}

/// Format bytes with the units and precision of Redis bytesToHuman.
fn bytes_to_human(bytes: u64) -> String {
    const UNITS: [char; 6] = ['B', 'K', 'M', 'G', 'T', 'P'];
    if bytes < 1024 {
        return format!("{bytes}B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{:.2}{}", value, UNITS[unit])
}

/// Seconds since the snapshot file was last written, zero without a snapshot.
async fn snapshot_modified_seconds(server: &RedisServer) -> u64 {
    let path = server
        .app
        .path
        .join("snapshot")
        .join(get_snapshot_file_name());
    let Ok(modified) = tokio::fs::metadata(path).await.and_then(|meta| meta.modified()) else {
        return 0;
    };
    modified
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;
    use crate::node::raft_node::RaftNode;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::broadcast;

    fn info_request(names: &[&str]) -> Vec<Value> {
        std::iter::once(Value::BulkString(Some(Bytes::from_static(b"INFO"))))
            .chain(
                names
                    .iter()
                    .map(|name| Value::BulkString(Some(Bytes::copy_from_slice(name.as_bytes())))),
            )
            .collect()
    }

    fn reply_text(reply: Value) -> String {
        let Value::VerbatimString { format, data } = reply else {
            panic!("INFO must reply with a verbatim string");
        };
        assert_eq!(format, "txt");
        String::from_utf8(data.to_vec()).unwrap()
    }

    #[test]
    fn sections_follow_redis_selection_rules() {
        assert_eq!(select_sections(&info_request(&[])), DEFAULT_SECTIONS.to_vec());
        assert!(select_sections(&info_request(&["unknown"])).is_empty());
        assert_eq!(
            select_sections(&info_request(&["MeMoRy"])),
            vec![Section::Memory]
        );
        assert_eq!(
            select_sections(&info_request(&["memory", "memory", "server"])),
            vec![Section::Memory, Section::Server]
        );
        for argument in ["default", "all", "everything"] {
            assert_eq!(
                select_sections(&info_request(&[argument])),
                DEFAULT_SECTIONS.to_vec()
            );
        }
    }

    #[test]
    fn human_bytes_match_redis_units() {
        assert_eq!(bytes_to_human(0), "0B");
        assert_eq!(bytes_to_human(1023), "1023B");
        assert_eq!(bytes_to_human(1024), "1.00K");
        assert_eq!(bytes_to_human(1536), "1.50K");
        assert_eq!(bytes_to_human(1024 * 1024), "1.00M");
    }

    #[tokio::test]
    async fn info_replies_with_redis_shaped_text() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.raft.log_path = dir.path().to_str().unwrap().to_owned();
        config.raft.address = "127.0.0.1:0".into();
        let config = ParsedConfig::from(&config).unwrap();
        let (shutdown_tx, _) = broadcast::channel(1);
        let node = RaftNode::create(config, shutdown_tx).await.unwrap();
        let server =
            RedisServer::new(node.app.clone(), "127.0.0.1:0".into(), &node.app.config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (connection, _) = listener.accept().await.unwrap();
        let mut client = Client::new(1, connection, true);

        let text = reply_text(
            InfoCommand
                .execute(&mut client, &info_request(&[]), &server)
                .await
                .unwrap(),
        );
        assert!(text.starts_with("# Server\r\n"));
        assert!(text.contains("redis_version:7.4.0\r\n"));
        assert!(text.contains("cache_cat_version:"));
        assert!(text.contains("role:"));
        assert!(text.contains("connected_slaves:"));
        assert!(text.contains("cluster_enabled:0\r\n"));
        assert!(text.contains("# Keyspace\r\n"));
        assert!(text.ends_with("\r\n\r\n"));

        let text = reply_text(
            InfoCommand
                .execute(&mut client, &info_request(&["memory"]), &server)
                .await
                .unwrap(),
        );
        assert!(text.starts_with("# Memory\r\n"));
        assert!(!text.contains("# Server"));

        let text = reply_text(
            InfoCommand
                .execute(&mut client, &info_request(&["unknown"]), &server)
                .await
                .unwrap(),
        );
        assert!(text.is_empty());

        node.app.cluster.shutdown().await.unwrap();
    }
}