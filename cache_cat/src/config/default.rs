use crate::config::config::{RaftConfig, RedisConfig, TlsConfig};

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
        tls_port: None,
        redis_port: 6379,
        requirepass: None,
        cleaning_interval: 10,
        sentinel_master_name: "cat".to_string(),
        databases: 16,
    }
}

pub fn default_tls_config() -> TlsConfig {
    TlsConfig {
        tls_cert_file: None,
        tls_key_file: None,
        tls_ca_cert_file: None,
        tls_auth_clients: None,
        tls_protocols: None,
        tls_replication: None,
    }
}
