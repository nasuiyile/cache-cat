use crate::config::config::Config;
use crate::error::Result;
use crate::node::parsed_config::ParsedConfig;
use crate::node::raft_node::RaftNode;
use crate::raft::application::cluster::NodeState;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::{Operation, Request};
use crate::utils::times::time_gap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::time;
use tracing::info;

pub struct RaftNodeBuilder;

impl RaftNodeBuilder {
    pub async fn build(config: &Config) -> Result<(Arc<RaftNode>, (Sender<()>, Receiver<()>))> {
        config.validate()?;
        let config = ParsedConfig::from(config)?;
        let cleaning_interval = config.cleaning_interval;

        let (tx, rx) = broadcast::channel(1);
        let raft_node = RaftNode::create(config, tx.clone()).await?;

        let arc = Arc::new(raft_node);
        RaftNode::start(arc.clone()).await?;
        let handle = arc.clone();
        timed_expiration(handle.clone(), cleaning_interval);
        let state_synchronization_interval = 1;
        cluster_state_sync(handle, state_synchronization_interval);
        Ok((arc, (tx, rx)))
    }
}

//主节点定期告诉所有节点当前集群的状态
pub fn cluster_state_sync(raft: Arc<RaftNode>, duration: u64) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(duration));
        loop {
            interval.tick().await;
            let Some(state) = raft.app.cluster.nodes_state() else {
                continue; // 没有值 代表不是leader，跳过本次循环
            };
            //设置自己的状态，然后同步给所有人
            raft.app.cluster.set_nodes_state(state.clone()).await;
            _ = raft.app.leader_rpc_call::<NodeState, ()>(12, state).await;
        }
    });
}

pub fn timed_expiration(raft: Arc<RaftNode>, duration: u64) {
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(duration));
        loop {
            interval.tick().await;
            let write_clock = raft.app.state_machine.data.kvs.get_write_clock();
            if time_gap(write_clock) < duration {
                continue;
            }
            if !raft.app.cluster.is_leader() {
                continue;
            }
            let mut have_expired = false;
            let kvs = &raft.app.state_machine.data.kvs;
            for x in &kvs.databases {
                if x.mocha.has_expired_by_local_clock_async().await {
                    have_expired = true;
                }
            }
            if !have_expired {
                continue;
            }
            info!("cleaning expired data");
            let res = raft
                .app
                .cluster
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
}
