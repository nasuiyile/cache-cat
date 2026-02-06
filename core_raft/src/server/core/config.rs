use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::OnceLock;

use crate::error::CoreRaftResult;

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub port: u16,
    pub log_level: String,
}

impl ServerConfig {
    pub fn from_file(path: &str) -> CoreRaftResult<Self> {
        let content = fs::read_to_string(path)?;
        let config: ServerConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

pub static CONFIG: OnceLock<ServerConfig> = OnceLock::new();

// 初始化函数
pub fn init_config(path: &str) -> CoreRaftResult<()> {
    let config = ServerConfig::from_file(path)?;
    CONFIG
        .set(config)
        .map_err(|_| crate::error::CoreRaftError::ConfigAlreadyInitialized)
}

// 获取配置的辅助函数
pub fn get_config() -> &'static ServerConfig {
    CONFIG.get().expect("Config not initialized")
}

// 使用示例
pub fn get_port() -> u16 {
    get_config().port
}
