use crate::config::config::Config;
use crate::error::Result;
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::raft_types::NodeId;
use openraft::SnapshotPolicy;
use std::cmp::max;

#[derive(Clone)]
pub struct ParsedConfig {
    pub node_id: NodeId,

    pub raft_endpoint: Endpoint,

    pub raft_advertise_endpoint: Endpoint,

    pub redis_port: u32,

    pub raft_single: bool,

    pub password: Option<String>,

    pub raft_join: Vec<String>,

    pub log_path: String,

    pub sentinel_master_name: String,

    /// 选举超时时间，节点之间的时钟偏移不能超过该值 需要大于500
    pub election_timeout: u64,
    /// 超过这个值将会直接进行快照，为0代表用不快照
    pub snapshot_policy: SnapshotPolicy,

    /// 超过这个阈值表示严重落后，需要大于snapshot_policy,防止快照还没生成。
    pub replication_lag_threshold: u64,

    /// 在没有请求到来时 多少秒进行一次key的清理 0表示不清理
    pub cleaning_interval: u64,

    pub db_number: u16,
}

impl ParsedConfig {
    pub fn from(config: &Config) -> Result<Self> {
        let raft_endpoint = Endpoint::parse(&config.raft.address, config.redis.redis_port)?;
        let raft_advertise_endpoint = Endpoint::new(
            &config.raft.advertise_host,
            config.redis.redis_port,
            raft_endpoint.port(),
        );
        let snapshot_policy = if config.raft.snapshot_policy == 0 {
            SnapshotPolicy::Never
        } else {
            SnapshotPolicy::LogsSinceLast(config.raft.snapshot_policy)
        };
        let election_timeout = max(config.raft.election_timeout, 500);
        Ok(ParsedConfig {
            password: config.redis.requirepass.clone(),
            node_id: config.node_id as NodeId,
            raft_endpoint,
            raft_advertise_endpoint,
            redis_port: config.redis.redis_port,
            raft_single: config.raft.single,
            raft_join: config.raft.join.clone(),
            log_path: config.raft.log_path.clone(),
            sentinel_master_name: config.redis.sentinel_master_name.clone(),
            election_timeout,
            snapshot_policy,
            replication_lag_threshold: config.raft.replication_lag_threshold,
            cleaning_interval: config.redis.cleaning_interval,
            db_number: config.redis.databases,
        })
    }
}
