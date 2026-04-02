use crate::network::model::{Request, Value};
use crate::network::router::{MultiNetworkFactory, Router};
use crate::server::client::file_client::FileOperator;
use crate::server::core::config::GROUP_NUM;
use crate::store::log_store::LogStore;
use crate::store::raft_engine::create_raft_engine;
use crate::store::store::StateMachineStore;
use openraft::Config;
use openraft::SnapshotPolicy::LogsSinceLast;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Value,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = FileOperator,
        NodeId=u16,
);
pub type GroupId = u16;
pub type NodeId = u16;

//实现是纯内存的暂时
pub type Raft = openraft::Raft<TypeConfig>;

pub struct CacheCatApp {
    pub node_id: NodeId,
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
pub fn get_group_by_key<'a>(app: &'a App, key: &Vec<u8>) -> &'a CacheCatApp {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    get_group(app, hasher.finish())
}
pub fn get_group_id_by_key(key: &Vec<u8>) -> GroupId {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % GROUP_NUM as u64) as GroupId
}

pub struct Node {
    pub node_id: NodeId,
    pub groups: HashMap<GroupId, CacheCatApp>,
}
impl Node {
    pub fn new(node_id: NodeId, addr: String) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
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
            node_id: self.node_id,
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
    let path = dir.as_ref().join("");
    let mut node = Node::new(node_id, addr.to_string());
    let raft_engine = dir.as_ref().join("raft-engine");
    let engine = create_raft_engine(raft_engine.clone());
    let config = Arc::new(Config {
        heartbeat_interval: 250,
        election_timeout_min: 299,
        election_timeout_max: 599, // 添加最大选举超时时间
        purge_batch_size: 1,
        max_in_snapshot_log_to_keep: 500, //生成快照后要保留的日志数量（以供从节点同步数据）需要大于等于replication_lag_threshold,该参数会影响快照逻辑
        max_append_entries: Some(50),
        max_payload_entries: 50,
        snapshot_policy: LogsSinceLast(100),
        replication_lag_threshold: 200, //需要大于snapshot_policy
        ..Default::default()
    });
    for i in 0..GROUP_NUM {
        let group_id = i as GroupId;
        // let engine_path = dir.as_ref().join(format!("raft-engine-{}", group_id));
        // let engine = create_raft_engine(engine_path);
        let router = Router::new(addr.to_string(), dir.as_ref().join(""), node_id);
        let network = MultiNetworkFactory::new(router, group_id);
        let log_store = LogStore::new(group_id, engine.clone());
        let sm_store = StateMachineStore::new(path.clone(), group_id, node_id)
            .await
            .unwrap();
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
