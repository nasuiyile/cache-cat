use crate::network::model::{Request, Value};
use crate::server::client::file_client::FileOperator;
use crate::server::core::config::GROUP_NUM;
use crate::store::store::StateMachineStore;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
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
