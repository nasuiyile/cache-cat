use crate::network::node::{GroupId, NodeId};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io;

pub const ONE: &str = "127.0.0.1:3001";
pub const TWO: &str = "127.0.0.1:3002";

pub const THREE: &str = "127.0.0.1:3003";

pub const TEMP_PATH: &str = r"E:\tmp\raft\raft-engine";

pub const GROUP_NUM: i16 = 1;
pub const TCP_CONNECT_NUM: u32 = 3;

pub const SNAPSHOT_FILE_NAME: &str = "snapshot";
pub fn get_snapshot_file_name(group_id: GroupId) -> String {
    format!("{}_{}.bin", SNAPSHOT_FILE_NAME, group_id)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub port: u16,
    pub log_level: String,
}

pub fn create_temp_dir() -> io::Result<PathBuf> {
    let path = Path::new(TEMP_PATH);
    // create_dir_all 是幂等的：目录存在不会报错
    fs::create_dir_all(path)?;
    Ok(path.to_path_buf())
}
pub fn default_raft_config() -> RaftConfig {
    RaftConfig {
        address: "127.0.0.1:6682".to_string(),
        advertise_host: "localhost".to_string(),
        single: true,
        join: vec![],
        log_path: "info".to_string(),
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Config {
    pub node_id: NodeId,
    pub redis_address: String,
    #[serde(default = "default_raft_config")]
    pub raft: RaftConfig,
}
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RaftConfig {
    pub address: String,

    pub advertise_host: String,

    /// Single node raft cluster.
    pub single: bool,

    /// Bring up a raft node and join a cluster.
    ///
    /// The value is one or more addresses of a node in the cluster, to which this node sends a `join` request.
    pub join: Vec<String>,

    pub log_path: String,
}
/// Load configuration from TOML file
pub fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

    let config: Config = toml::from_str(&config_str)
        .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

    Ok(config)
}
