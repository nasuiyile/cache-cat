// 放在 cache_cat::config::config 模块中（或在 main.rs 附近新建 cli_config.rs）
use crate::config::config::{Config, load_config};
use clap::Parser;
use std::path::PathBuf;

/// 命令行参数定义
/// 命令行参数定义 - 所有 Config 字段都可被覆盖
#[derive(Parser, Debug)]
#[command(
    name = "cache-cat",
    version,
    about = "CacheCat - Raft-based distributed cache"
)]
pub struct CliArgs {
    /// 配置文件路径（可选）
    #[arg(short, long = "conf")]
    pub config: Option<PathBuf>,

    /// 节点 ID
    #[arg(long = "node_id")]
    pub node_id: Option<u16>,

    // Redis 配置
    /// Redis 端口
    #[arg(long = "redis-port")]
    pub redis_port: Option<u32>,

    /// Redis 密码
    #[arg(long = "requirepass")]
    pub redis_password: Option<String>,

    /// Redis 清理间隔（秒）
    #[arg(long = "cleaning_interval")]
    pub redis_cleaning_interval: Option<u64>,

    /// Redis Sentinel 主节点名称
    #[arg(long = "sentinel_master")]
    pub redis_sentinel_master_name: Option<String>,

    /// Redis 数据库数量
    #[arg(long = "redis_databases")]
    pub redis_databases: Option<u16>,

    // Raft 配置
    /// Raft 日志路径
    #[arg(long = "log_path")]
    pub raft_log_path: Option<String>,

    /// Raft 监听地址
    #[arg(long = "address")]
    pub raft_address: Option<String>,

    /// Raft 广播地址
    #[arg(long = "advertise_host")]
    pub raft_advertise_host: Option<String>,

    /// 单节点模式
    #[arg(long = "single")]
    pub raft_single: Option<bool>,

    /// 加入集群的节点地址（可多次指定）
    #[arg(long = "join")]
    pub raft_join: Vec<String>,

    /// 选举超时时间（毫秒）
    #[arg(long = "election_timeout")]
    pub raft_election_timeout: Option<u64>,

    /// 快照策略（日志条目数，0 表示禁用）
    #[arg(long = "snapshot_policy")]
    pub raft_snapshot_policy: Option<u64>,

    /// 复制滞后阈值（日志条目数）
    #[arg(long = "replication_lag_threshold")]
    pub raft_replication_lag_threshold: Option<u64>,
}

/// 合并配置：配置文件（可选） + 命令行覆盖
pub fn load_config_with_cli() -> Result<Config, Box<dyn std::error::Error>> {
    let cli = CliArgs::parse();
    // 基础配置：如果提供了配置文件则加载，否则使用默认 Config
    let mut config = if let Some(path) = &cli.config {
        let path_str = path.to_str()
            .ok_or_else(|| format!("Invalid config file path: {:?}", path))?;
        load_config(path_str)?
    } else {
        Config::default()
    };

    // 命令行覆盖 - 仅当对应参数被显式指定时才覆盖
    if let Some(v) = cli.node_id {
        config.node_id = v;
    }

    // Redis 配置覆盖
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

    // Raft 配置覆盖
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

    // 验证配置
    config
        .validate()
        .map_err(|e| format!("Configuration validation failed: {}", e))?;

    Ok(config)
}
