use std::fs;
use std::net::SocketAddr;

use serde::Deserialize;
use serde::Serialize;

use super::default::default_raft_config;
use crate::error::{Error, Result};
use std::result::Result as StdResult;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct Config {
    pub node_id: u64,

    #[serde(default = "default_raft_config")]
    pub raft: RaftConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
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
}

impl Config {
    /// Validate the configuration to ensure it is correct.
    pub fn validate(&self) -> Result<()> {
        if self.raft.single && !self.raft.join.is_empty() {
            return Err(Error::config(
                "'single' mode cannot be used together with 'join' configuration",
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
/// Load configuration from TOML file
pub fn load_config(path: &str) -> StdResult<Config, Box<dyn std::error::Error>> {
    let config_str = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

    let config: Config = toml::from_str(&config_str)
        .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

    Ok(config)
}
