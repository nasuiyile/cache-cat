use super::default::default_node_id;
use super::default::default_raft_config;
use super::default::default_redis_config;
use super::default::default_tls_config;
use crate::error::{Error, Result};
use serde::Deserialize;
use serde::Serialize;
use std::fs;
use std::net::SocketAddr;
use std::result::Result as StdResult;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(default = "default_node_id")]
    pub node_id: u16,

    #[serde(default = "default_redis_config")]
    pub redis: RedisConfig,

    #[serde(default = "default_raft_config")]
    pub raft: RaftConfig,

    /// TLS configuration (optional)
    #[serde(default = "default_tls_config")]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RedisConfig {
    /// TLS监听端口
    pub tls_port: Option<u32>,

    pub redis_port: u32,

    pub requirepass: Option<String>,

    /// 在没有请求到来时 多少秒进行一次key的清理 0表示不清理
    pub cleaning_interval: u64,

    pub sentinel_master_name: String,

    pub databases: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftConfig {
    pub log_path: String,

    pub address: String,

    pub advertise_host: String,

    /// Single node raft cluster.
    pub single: bool,

    /// Bring up a raft node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    pub join: Vec<String>,

    /// 选举超时时间，节点之间的时钟偏移不能超过该值 需要大于500
    pub election_timeout: u64,

    /// 超过这个值将会直接进行快照，为0代表不用快照
    pub snapshot_policy: u64,

    /// 超过这个阈值表示严重落后，需要大于snapshot_policy,防止快照还没生成。
    pub replication_lag_threshold: u64,
}

/// TLS configuration
///
/// 所有字段均允许为空
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub struct TlsConfig {
    /// 服务端证书
    pub tls_cert_file: Option<String>,

    /// 服务端私钥
    pub tls_key_file: Option<String>,

    /// CA证书
    pub tls_ca_cert_file: Option<String>,

    /// 是否要求客户端证书
    pub tls_auth_clients: Option<bool>,

    /// TLS协议版本，例如 "TLSv1.2 TLSv1.3"
    pub tls_protocols: Option<String>,

    /// Raft复制是否启用TLS
    pub tls_replication: Option<bool>,
}

impl Config {
    /// Validate the configuration to ensure it is correct.
    pub fn validate(&self) -> Result<()> {
        if self.raft.single && !self.raft.join.is_empty() {
            return Err(Error::config(
                "'single' mode cannot be used together with 'join' configuration",
            ));
        }

        if self.raft.snapshot_policy != 0
            && self.raft.snapshot_policy > self.raft.replication_lag_threshold
        {
            return Err(Error::config(
                "'snapshot_policy' cannot be greater than 'replication_lag_threshold'",
            ));
        }

        let _a: SocketAddr = self
            .raft
            .address
            .parse()
            .map_err(|e| Error::config(format!("{} while parsing {}", e, self.raft.address)))?;

        Ok(())
    }
}

/// Load configuration from TOML file
pub fn load_config(path: &str) -> StdResult<Config, Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

    let config: Config = toml::from_str(&config_str)
        .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

    Ok(config)
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            redis: default_redis_config(),
            raft: default_raft_config(),
            tls: default_tls_config(),
        }
    }
}
