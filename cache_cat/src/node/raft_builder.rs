use crate::config::config::Config;
use crate::error::Result;
use crate::node::node::RaftNode;

use crate::node::parsed_config::ParsedConfig;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Request;
use crate::utils::times::time_gap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time;
use tracing::error;

pub struct RaftNodeBuilder;

impl RaftNodeBuilder {
    /// Build a new RaftNode with the given configuration
    pub async fn build(config: &Config) -> Result<Arc<RaftNode>> {
        config.validate()?;
        let config = ParsedConfig::from(config)?;
        let duration = config.cleaning_interval;
        let raft_node = RaftNode::create(config).await?;
        let arc = Arc::from(raft_node);
        RaftNode::start(arc.clone()).await?;
        let back = arc.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(duration));
            loop {
                interval.tick().await;
                for cache in &back.app.state_machine.data.kvs.cache {
                    cache.run_pending_tasks()
                }
                let write_clock = back.app.state_machine.data.kvs.get_write_clock();
                let have_deleted = back
                    .app
                    .state_machine
                    .data
                    .kvs
                    .have_deleted
                    .load(Ordering::Acquire);

                //超过阈值没有任何的写操作 并且这期间存在数据被删除过。那么就提交一条空日志给从节点推动时钟前进
                if time_gap(write_clock) > duration && have_deleted {
                    let result = back
                        .app
                        .raft
                        .client_write(Request::new_base(write_clock, 0, BaseOperation::Empty))
                        .await;
                    match result {
                        Err(err) => {
                            error!("Empty log submission failed error: {}", err);
                        }
                        _ => {
                            back.app
                                .state_machine
                                .data
                                .kvs
                                .have_deleted
                                .store(false, Ordering::Release);
                        }
                    }
                }
            }
        });
        Ok(arc)
    }
}
