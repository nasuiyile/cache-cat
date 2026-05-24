use crate::config::config::Config;
use crate::error::Result;
use crate::node::node::RaftNode;
use crate::node::parsed_config::ParsedConfig;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, Request};
use crate::utils::times::time_gap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::info;

pub struct RaftNodeBuilder;

impl RaftNodeBuilder {
    pub async fn build(config: &Config) -> Result<Arc<RaftNode>> {
        config.validate()?;
        let config = ParsedConfig::from(config)?;
        let duration = 1;

        let raft_node = RaftNode::create(config).await?;
        let arc = Arc::new(raft_node);

        RaftNode::start(arc.clone()).await?;

        // 只 clone 一次用于后台任务
        let cleanup_handle = arc.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let write_clock = cleanup_handle.app.state_machine.data.kvs.get_write_clock();

                if time_gap(write_clock) < duration {
                    continue;
                }
                if !cleanup_handle.app.raft.is_leader() {
                    continue;
                }
                let mut have_expired = false;
                let kvs = &cleanup_handle.app.state_machine.data.kvs;
                for x in &kvs.databases {
                    if x.mocha.has_expired_by_local_clock_async().await {
                        have_expired = true;
                    }
                }
                if !have_expired {
                    continue;
                }
                info!("cleaning expired data");
                let res = cleanup_handle
                    .app
                    .raft
                    .client_write(Request::new(
                        kvs.generate_new_write_clock(),
                        0,
                        Operation::Base(BaseOperation::Empty),
                    ))
                    .await;
                if res.is_err() {
                    info!("cleaning expired data failed");
                }
            }
        });

        Ok(arc) // 直接返回，无需再 clone
    }
}
