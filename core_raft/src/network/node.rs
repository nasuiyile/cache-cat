use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use crate::network::router::{MultiNetworkFactory, Router};
use crate::store::rocks_store::{StateMachineData, new_storage};
use openraft::Config;
use openraft_multi::GroupNetworkFactory;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
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

const GROUP_NUM: i16 = 2;
pub struct CacheCatApp {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub group_id: GroupId,
    pub state_machine: StateMachineStore,
}
pub type App = Arc<Vec<Box<CacheCatApp>>>;
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
    ) {
        let app = CacheCatApp {
            id: self.node_id,
            addr: addr.to_string(),
            raft,
            group_id,
            state_machine,
        };
        self.groups.insert(group_id, app);
    }
}

pub async fn create_node<P>(addr: &str, node_id: NodeId, dir: P) -> Node
where
    P: AsRef<Path>,
{
    let mut node = Node::new(node_id, addr.to_string());
    for i in 0..GROUP_NUM {
        let group_id = i as GroupId;
        let group_dir: PathBuf = Path::new(dir.as_ref()).join(i.to_string());
        fs::create_dir_all(&group_dir).await.unwrap();
        let config = Arc::new(Config {
            heartbeat_interval: 2500,
            election_timeout_min: 2990,
            election_timeout_max: 5990, // 添加最大选举超时时间
            ..Default::default()
        });
        let router = Router::new(addr.to_string());
        let network = MultiNetworkFactory::new(router.clone(), group_id);
        let (log_store, state_machine_store) = new_storage(&group_dir).await;
        let raft = openraft::Raft::new(
            node_id,
            config.clone(),
            network,
            log_store,
            state_machine_store.clone(),
        )
        .await
        .unwrap();
        node.add_group(addr, group_id, raft, state_machine_store)
    }
    node
}
