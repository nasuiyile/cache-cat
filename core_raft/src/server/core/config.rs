use crate::network::node::GroupId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io;

pub const ONE: &str = "127.0.0.1:3001";
pub const TWO: &str = "127.0.0.1:3002";

pub const THREE: &str = "127.0.0.1:3003";

pub const TEMP_PATH: &str = r"E:\tmp\raft\raft-engine";

pub const GROUP_NUM: i16 = 2;
pub const TCP_CONNECT_NUM: u32 = 3;

pub const SNAPSHOT_FILE_NAME: &str = "snapshot";
pub fn get_snapshot_file_name(group_id: GroupId) -> String {
    format!("{}_{}.snapshot", SNAPSHOT_FILE_NAME, group_id)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub port: u16,
    pub log_level: String,
}

impl ServerConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: ServerConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

pub static CONFIG: OnceLock<ServerConfig> = OnceLock::new();

// 初始化函数
pub fn init_config(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::from_file(path)?;
    CONFIG
        .set(config)
        .map_err(|_| "Config already initialized".into())
}

// 获取配置的辅助函数
pub fn get_config() -> &'static ServerConfig {
    CONFIG.get().expect("Config not initialized")
}

// 使用示例
pub fn get_port() -> u16 {
    get_config().port
}
pub fn create_temp_dir() -> io::Result<PathBuf> {
    let path = Path::new(TEMP_PATH);
    // create_dir_all 是幂等的：目录存在不会报错
    fs::create_dir_all(path)?;
    Ok(path.to_path_buf())
}
