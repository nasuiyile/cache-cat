use crate::cfg::config::{RaftConfig, RedisConfig, TlsConfig};

#[inline]
pub fn default_advertise_host() -> String {
    "0.0.0.0".to_string()
}

#[inline]
pub fn default_raft_config() -> RaftConfig {
    RaftConfig {
        log_path: ".data".to_string(),
        address: "0.0.0.0:5001".to_string(),
        single: true,
        join: vec![],
        election_timeout: 699,
        snapshot_policy: 50000,
        replication_lag_threshold: 60000,
    }
}

#[inline]
pub const fn default_node_id() -> u16 {
    1
}

pub fn default_redis_config() -> RedisConfig {
    RedisConfig {
        bind_address: None,
        tls_port: None,
        redis_port: 6379,
        requirepass: None,
        cleaning_interval: 10,
        sentinel_master_name: "cat".to_string(),
        databases: 16,
    }
}

#[inline]
pub const fn default_tls_config() -> TlsConfig {
    TlsConfig {
        tls_cert_file: None,
        tls_key_file: None,
        tls_ca_cert_file: None,
        tls_auth_clients: None,
        tls_protocols: None,
        tls_replication: None,
    }
}
