use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::network::router::{MultiNetworkFactory, Router};
use crate::server::core::config::GROUP_NUM;
use crate::store::raft_engine::create_raft_engine;
use crate::store::rocks_log_store::RocksLogStore;
use crate::store::rocks_store::{StateMachineData, new_storage};
use openraft::Config;
use openraft::SnapshotPolicy::{LogsSinceLast, Never};
use rocksdb::{DB, DBWithThreadMode};
use serde::de::Unexpected::Option;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        NodeId=u16,
);
pub type GroupId = u16;
pub type NodeId = u16;

//实现是纯内存的暂时
pub type LogStore = crate::store::rocks_log_store::RocksLogStore;
pub type StateMachineStore = crate::store::rocks_store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;

pub struct CacheCatApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub group_id: GroupId,
    pub state_machine: StateMachineStore,
    pub path: PathBuf,
}
pub type App = Arc<Vec<Arc<CacheCatApp>>>;
pub fn get_app(app: &App, group_id: GroupId) -> &CacheCatApp {
    app.iter().find(|app| app.group_id == group_id).unwrap()
}
pub fn get_group(app: &App, hash_code: u64) -> &CacheCatApp {
    let usize = hash_code % app.len() as u64;
    get_app(app, usize as GroupId)
}

pub struct Node {
    pub node_id: NodeId,
    pub groups: HashMap<GroupId, CacheCatApp>,
    pub router: Router,
}
impl Node {
    pub fn new(node_id: NodeId, addr: String) -> Self {
        let router = Router::new(addr.clone());
        Self {
            node_id,
            groups: HashMap::new(),
            router,
        }
    }
    pub fn add_group(
        &mut self,
        addr: &str,
        group_id: GroupId,
        raft: Raft,
        state_machine: StateMachineStore,
        path: PathBuf,
    ) {
        let app = CacheCatApp {
            id: self.node_id,
            addr: addr.to_string(),
            raft,
            group_id,
            state_machine,
            path,
        };
        self.groups.insert(group_id, app);
    }
}

pub async fn create_node<P>(addr: &str, node_id: NodeId, dir: P) -> Node
where
    P: AsRef<Path>,
{
    let rocksdb_path = dir.as_ref().join("rocksdb");
    let db: Arc<DB> = new_storage(rocksdb_path).await;
    let mut node = Node::new(node_id, addr.to_string());
    let raft_engine = dir.as_ref().join("raft-engine");
    let engine = create_raft_engine(raft_engine.clone());
    let config = Arc::new(Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        election_timeout_max: 599, // 添加最大选举超时时间
        max_payload_entries: 10000000,
        purge_batch_size: 10000000,
        max_append_entries: Some(10000000),
        snapshot_policy: LogsSinceLast(100),
        replication_lag_threshold: 10000000, //需要大于snapshot_policy
        ..Default::default()
    });
    for i in 0..GROUP_NUM {
        let group_id = i as GroupId;
        // let raft_engine = dir.as_ref().join(format!("raft-engine-{}", group_id));
        // let path = if i == 1 {
        // let path = TempDir::new_in(r"E:\tmp\raft\raft-engine").unwrap().into_path();
        // } else {
        //     TempDir::new_in(r"E:\tmp\raft\raft-engine").unwrap().into_path()
        // };
        // let engine = create_raft_engine(path);
        let router = Router::new(addr.to_string());
        let network = MultiNetworkFactory::new(router, group_id);
        let log_store = RocksLogStore::new(group_id, engine.clone());
        let sm_store = StateMachineStore::new(db.clone(), group_id).await.unwrap();
        let raft = openraft::Raft::new(
            node_id,
            config.clone(),
            network,
            log_store,
            sm_store.clone(),
        )
        .await
        .unwrap();
        node.add_group(addr, group_id, raft, sm_store, dir.as_ref().join(""))
    }
    node
}
