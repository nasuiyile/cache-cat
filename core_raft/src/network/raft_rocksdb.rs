use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::network::node::{App, CacheCatApp, NodeId, create_node};
use crate::server::handler::rpc;
use crate::store::rocks_store::new_storage;
use openraft::{BasicNode, Config};
use std::collections::BTreeMap;

use crate::network::router::Router;
use std::path::Path;
use std::sync::Arc;

pub async fn start_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let config = Arc::new(Config {
        heartbeat_interval: 2500,
        election_timeout_min: 2990,
        election_timeout_max: 5990, // 添加最大选举超时时间
        ..Default::default()
    });

    let (log_store, state_machine_store) = new_storage(&dir).await;
    let network = NetworkFactory {};

    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    let app = CacheCatApp {
        id: node_id,
        addr: addr.clone(),
        raft,
        group_id: 0,
        state_machine: state_machine_store,
    };

    // 正确构建集群成员映射
    let mut nodes = BTreeMap::new();
    if node_id == 3 {
        nodes.insert(
            1,
            BasicNode {
                addr: "127.0.0.1:3001".to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: "127.0.0.1:3002".to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: "127.0.0.1:3003".to_string(),
            },
        );
        app.raft.initialize(nodes).await.unwrap();
    }
    // 根据node_id决定完整的集群配置

    rpc::start_server(App::new(vec![Box::new(app)]), addr).await
}
pub async fn start_multi_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let node = create_node(&addr, node_id, dir).await;
    let apps: Vec<Box<CacheCatApp>> = node.groups.into_values().map(Box::new).collect();
    rpc::start_server(App::new(apps), addr).await
}
