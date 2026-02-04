use crate::network::model::{WriteReq, WriteResRaft};
use crate::network::network::NetworkFactory;
use crate::server::handler::model::SetReq;
use crate::server::handler::rpc;
use crate::store::rocks_store::new_storage;
use openraft::{BasicNode, Config};
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = WriteReq,
        R = WriteResRaft,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        
);
//实现是纯内存的暂时
pub type LogStore = crate::store::rocks_log_store::RocksLogStore;
pub type StateMachineStore = crate::store::rocks_store::StateMachineStore;
pub type Raft = openraft::Raft<TypeConfig>;
pub async fn start_raft_app<P>(node_id: u64, dir: P, addr: String) -> std::io::Result<()>
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
    let kvs = state_machine_store.data.kvs.clone();
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
        config,
        key_values: kvs,
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

    //
    //
    // let request = Request::Set(SetReq {
    //     key: "key".to_string(),
    //     value: "key".to_string().into(),
    //     ex_time: 100000,
    // });
    // app.raft.client_write(request).await.unwrap();

    rpc::start_server(Arc::new(app)).await
}
pub struct CacheCatApp {
    pub id: u64,
    pub addr: String,
    pub raft: Raft,
    pub config: Arc<Config>,
    pub key_values: Arc<Mutex<HashMap<String, String>>>,
}
