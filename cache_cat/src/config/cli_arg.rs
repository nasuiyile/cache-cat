use crate::config::config::{Config, load_config};
use clap::Parser;
use std::path::PathBuf;

/// Command-line arguments.
///
/// Values provided on the command line override values loaded from the
/// configuration file.
#[derive(Parser, Debug)]
#[command(
    name = "cache-cat",
    version,
    about = "CacheCat - Raft-based distributed cache"
)]
pub struct CliArgs {
    /// Path to the configuration file.
    #[arg(short, long = "conf")]
    pub config: Option<PathBuf>,

    /// Node ID.
    #[arg(long = "node-id")]
    pub node_id: Option<u16>,

    // ------------------------------------------------------------------------
    // Redis
    // ------------------------------------------------------------------------
    /// Redis port.
    #[arg(long = "redis-port")]
    pub redis_port: Option<u32>,

    /// Redis password.
    #[arg(long = "requirepass")]
    pub redis_password: Option<String>,

    /// Background key cleaning interval in seconds.
    #[arg(long = "cleaning-interval")]
    pub redis_cleaning_interval: Option<u64>,

    /// Redis Sentinel master name.
    #[arg(long = "sentinel-master-name")]
    pub redis_sentinel_master_name: Option<String>,

    /// Number of Redis databases.
    #[arg(long = "redis-databases")]
    pub redis_databases: Option<u16>,

    // ------------------------------------------------------------------------
    // Raft
    // ------------------------------------------------------------------------
    /// Raft log directory.
    #[arg(long = "log-path")]
    pub raft_log_path: Option<String>,

    /// Raft listening address.
    #[arg(long = "address")]
    pub raft_address: Option<String>,

    /// Raft advertised host.
    #[arg(long = "advertise-host")]
    pub raft_advertise_host: Option<String>,

    /// Enable single-node mode.
    #[arg(long = "single")]
    pub raft_single: Option<bool>,

    /// Join one or more existing cluster nodes.
    #[arg(long = "join")]
    pub raft_join: Vec<String>,

    /// Raft election timeout in milliseconds.
    #[arg(long = "election-timeout")]
    pub raft_election_timeout: Option<u64>,

    /// Snapshot policy threshold.
    #[arg(long = "snapshot-policy")]
    pub raft_snapshot_policy: Option<u64>,

    /// Replication lag threshold.
    #[arg(long = "replication-lag-threshold")]
    pub raft_replication_lag_threshold: Option<u64>,

    // ------------------------------------------------------------------------
    // TLS
    // ------------------------------------------------------------------------
    /// TLS listening port.
    #[arg(long = "tls-port")]
    pub tls_port: Option<u32>,

    /// Server certificate file.
    #[arg(long = "tls-cert-file")]
    pub tls_cert_file: Option<String>,

    /// Server private key file.
    #[arg(long = "tls-key-file")]
    pub tls_key_file: Option<String>,

    /// CA certificate file.
    #[arg(long = "tls-ca-cert-file")]
    pub tls_ca_cert_file: Option<String>,

    /// Require client certificates.
    #[arg(long = "tls-auth-clients")]
    pub tls_auth_clients: Option<bool>,

    /// Enabled TLS protocol versions.
    #[arg(long = "tls-protocols")]
    pub tls_protocols: Option<String>,

    /// Enable TLS for Raft replication.
    #[arg(long = "tls-replication")]
    pub tls_replication: Option<bool>,
}

/// Load configuration from the configuration file and apply command-line
/// overrides.
pub fn load_config_with_cli() -> Result<Config, Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();

    // Load configuration file if provided; otherwise use defaults.
    let mut config = if let Some(path) = &cli.config {
        let path_str = path
            .to_str()
            .ok_or_else(|| format!("Invalid config file path: {:?}", path))?;

        load_config(path_str)?
    } else {
        Config::default()
    };

    // ------------------------------------------------------------------------
    // Global
    // ------------------------------------------------------------------------

    if let Some(v) = cli.node_id {
        config.node_id = v;
    }

    // ------------------------------------------------------------------------
    // Redis
    // ------------------------------------------------------------------------

    if let Some(v) = cli.redis_port {
        config.redis.redis_port = v;
    }

    if let Some(v) = cli.redis_password {
        config.redis.requirepass = Some(v);
    }

    if let Some(v) = cli.redis_cleaning_interval {
        config.redis.cleaning_interval = v;
    }

    if let Some(v) = cli.redis_sentinel_master_name {
        config.redis.sentinel_master_name = v;
    }

    if let Some(v) = cli.redis_databases {
        config.redis.databases = v;
    }

    // ------------------------------------------------------------------------
    // Raft
    // ------------------------------------------------------------------------

    if let Some(v) = cli.raft_log_path {
        config.raft.log_path = v;
    }

    if let Some(v) = cli.raft_address {
        config.raft.address = v;
    }

    if let Some(v) = cli.raft_advertise_host {
        config.raft.advertise_host = v;
    }

    if let Some(v) = cli.raft_single {
        config.raft.single = v;
    }

    if !cli.raft_join.is_empty() {
        config.raft.join = cli.raft_join;
    }

    if let Some(v) = cli.raft_election_timeout {
        config.raft.election_timeout = v;
    }

    if let Some(v) = cli.raft_snapshot_policy {
        config.raft.snapshot_policy = v;
    }

    if let Some(v) = cli.raft_replication_lag_threshold {
        config.raft.replication_lag_threshold = v;
    }

    // ------------------------------------------------------------------------
    // TLS
    // ------------------------------------------------------------------------

    if let Some(v) = cli.tls_port {
        config.redis.tls_port = Some(v);
    }

    if let Some(v) = cli.tls_cert_file {
        config.tls.tls_cert_file = Some(v);
    }

    if let Some(v) = cli.tls_key_file {
        config.tls.tls_key_file = Some(v);
    }

    if let Some(v) = cli.tls_ca_cert_file {
        config.tls.tls_ca_cert_file = Some(v);
    }

    if let Some(v) = cli.tls_auth_clients {
        config.tls.tls_auth_clients = Some(v);
    }

    if let Some(v) = cli.tls_protocols {
        config.tls.tls_protocols = Some(v);
    }

    if let Some(v) = cli.tls_replication {
        config.tls.tls_replication = Some(v);
    }

    // Validate the final configuration.
    config
        .validate()
        .map_err(|e| format!("Configuration validation failed: {}", e))?;

    Ok(config)
}
