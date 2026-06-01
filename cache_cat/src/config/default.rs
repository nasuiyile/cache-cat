use crate::config::config::{RaftConfig, RedisConfig};

pub fn default_raft_config() -> RaftConfig {
    RaftConfig {
        log_path: ".data".to_string(),
        address: "127.0.0.1:5001".to_string(),
        advertise_host: "localhost".to_string(),
        single: true,
        join: vec![],
        election_timeout: 699,
        snapshot_policy: 50000,
        replication_lag_threshold: 60000,
    }
}
pub fn default_node_id() -> u16 {
    1
}

pub fn default_redis_config() -> RedisConfig {
    RedisConfig {
        redis_port: 6379,
        requirepass: None,
        cleaning_interval: 10,
        sentinel_master_name: "cat".to_string(),
        databases: 16,
    }
}
