use crate::config::config::Config;
use crate::error::Result;
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::raft_types::NodeId;

pub struct ParsedConfig {
    pub node_id: NodeId,

    pub raft_endpoint: Endpoint,

    pub raft_advertise_endpoint: Endpoint,

    pub redis_addr: String,

    pub raft_single: bool,

    pub raft_join: Vec<String>,

    #[allow(dead_code)]
    pub rocksdb_data_path: String,
}

impl ParsedConfig {
    pub fn from(config: &Config) -> Result<Self> {
        let raft_endpoint = Endpoint::parse(&config.raft.address)?;
        let raft_advertise_endpoint =
            Endpoint::new(&config.raft.advertise_host, raft_endpoint.port());

        Ok(ParsedConfig {
            node_id: config.node_id as NodeId,
            raft_endpoint,
            raft_advertise_endpoint,
            redis_addr: config.redis_addr.clone(),
            raft_single: config.raft.single,
            raft_join: config.raft.join.clone(),
            rocksdb_data_path: config.raft.log_path.clone(),
        })
    }
}
