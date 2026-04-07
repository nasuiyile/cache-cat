use crate::config::config::Config;
use crate::error::{Error, Result};
use crate::node::parsed_config::ParsedConfig;
use crate::raft::network::router::{MultiNetworkFactory, Router};
use crate::raft::network::rpc::Server;
use crate::raft::store::log_store::LogStore;
use crate::raft::store::raft_engine::create_raft_engine;
use crate::raft::store::statemachine::{StateMachineData, StateMachineStore};
use crate::raft::types::raft_types::{
    App, CacheCatApp, GROUP_NUM, GroupId, Node, NodeId, Raft, TypeConfig,
};
use openraft::SnapshotPolicy::Never;
use openraft::error::{InitializeError, RaftError};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, oneshot};
use tracing::{error, info};

pub struct RaftNode {
    config: ParsedConfig,

    pub groups: Mutex<HashMap<GroupId, CacheCatApp>>,

    shutdown_tx: broadcast::Sender<()>,
    _shutdown_rx: broadcast::Receiver<()>,
    service_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RaftNode {
    pub async fn create(app_config: &Config) -> Result<RaftNode> {
        let node_id = app_config.node_id as NodeId;
        let dir = Path::new(&app_config.raft.log_path);
        let path = dir.join("");
        let (shutdown_tx, shutdown_rx_for_struct) = broadcast::channel(1);
        let mut node = Self {
            config: ParsedConfig::from(app_config)?,
            groups: Default::default(),
            shutdown_tx,
            _shutdown_rx: shutdown_rx_for_struct,
            service_handle: Mutex::new(None),
        };
        let raft_engine = dir.join("raft-engine");
        let engine = create_raft_engine(raft_engine.clone());
        let config = Arc::new(openraft::Config {
            heartbeat_interval: 250,
            election_timeout_min: 299,
            election_timeout_max: 599, // 添加最大选举超时时间
            purge_batch_size: 1,
            max_in_snapshot_log_to_keep: 500, //生成快照后要保留的日志数量（以供从节点同步数据）需要大于等于replication_lag_threshold,该参数会影响快照逻辑
            max_append_entries: Some(5000000),
            max_payload_entries: 5000000,
            snapshot_policy: Never,         //LogsSinceLast(100),
            replication_lag_threshold: 200, //需要大于snapshot_policy
            ..Default::default()
        });
        for i in 0..GROUP_NUM {
            let group_id = i as GroupId;
            // let engine_path = dir.as_ref().join(format!("raft-engine-{}", group_id));
            // let engine = create_raft_engine(engine_path);
            let router = Router::new(app_config.raft.address.clone(), dir.join(""), node_id);
            let network = MultiNetworkFactory::new(router, group_id);
            let log_store = LogStore::new(group_id, engine.clone());
            let sm_store = StateMachineStore::new(path.clone(), group_id, node_id).await?;
            let raft = openraft::Raft::new(
                node_id,
                config.clone(),
                network,
                log_store,
                sm_store.clone(),
            )
            .await
            .map_err(|e| Error::internal(format!("Failed to create raft: {}", e)))?;
            node.add_group(
                &*app_config.raft.address,
                group_id,
                raft,
                sm_store,
                dir.join(""),
            )
        }
        Ok(node)
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
            node_id: self.config.node_id,
            addr: addr.to_string(),
            raft,
            group_id,
            state_machine,
            path,
        };
        self.groups.lock().unwrap().insert(group_id, app);
    }
    pub async fn start(raft_node: Arc<Self>) -> Result<()> {
        let config = &raft_node.config;
        Self::start_raft_service(raft_node.clone()).await?;
        if config.raft_single {
            let node = Node {
                node_id: config.node_id,
                endpoint: config.raft_endpoint.clone(),
            };
            raft_node.init_cluster(node).await?;
        } else {
            raft_node.join_cluster().await?;
        }
        Ok(())
    }

    pub async fn join_cluster(&self) -> Result<()> {
        Ok(())
    }

    /// Initialize the Raft cluster with a single node
    /// * `Ok(())` - Successfully initialized the cluster
    /// * `Err(Error)` - Failed to initialize:
    ///   - `InvalidConfig` if node configuration is invalid
    ///   - `Internal` if adding node to cluster fails
    async fn init_cluster(&self, node: Node) -> Result<()> {
        let groups = self.groups.lock().unwrap();
        for (_, app) in groups.iter() {
            if node.node_id != *app.raft.node_id() {
                return Err(Error::config(format!(
                    "Node ID {} does not match current node ID {}",
                    node.node_id,
                    app.raft.node_id()
                )));
            }
        }

        // Validate endpoint
        if node.endpoint.addr().is_empty() {
            return Err(Error::config("Node endpoint address cannot be empty"));
        }

        info!("Node {} added to state machine successfully", node.node_id);

        // Initialize cluster with the node
        let node_id = node.node_id;
        let mut nodes = BTreeMap::new();
        nodes.insert(node_id, node);

        for (_, app) in groups.iter() {
            if let Err(e) = app.raft.initialize(nodes.clone()).await {
                match e {
                    RaftError::APIError(e) => match e {
                        InitializeError::NotAllowed(e) => {
                            info!("Already initialized: {}", e);
                        }
                        InitializeError::NotInMembers(e) => {
                            return Err(Error::config(e.to_string()));
                        }
                    },
                    RaftError::Fatal(e) => {
                        return Err(Error::internal(e.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
        let raft_endpoint = raft_node.config.raft_endpoint.clone();

        // Subscribe to shutdown signal
        let shutdown_rx = raft_node.shutdown_tx.subscribe();

        // Create oneshot channel to signal startup completion
        let (startup_tx, startup_rx) = oneshot::channel::<StdResult<(), String>>();

        let apps: Vec<Arc<CacheCatApp>> = raft_node
            .groups
            .lock()
            .unwrap()
            .drain()
            .map(|(_, app)| Arc::new(app))
            .collect();

        let addr = raft_node.config.raft_advertise_endpoint.to_string();
        let handle = tokio::task::spawn(async move {
            // Signal startup success
            let server = Server {
                app: App::new(apps),
                addr,
                startup_tx,
            };
            if let Err(e) = server.start_server(shutdown_rx).await {
                error!("Server error: {}", e);
            }
            info!("TCP Service task finished");
        });
        // Wait for startup completion signal
        match startup_rx.await {
            Ok(Ok(())) => {
                // Store the handle in RaftNode
                *raft_node.service_handle.lock().unwrap() = Some(handle);
                info!("Raft TCP service started successfully");
                Ok(())
            }
            Ok(Err(err_msg)) => {
                // Wait for the task to finish to ensure proper cleanup
                let _ = handle.await;
                Err(Error::internal(err_msg))
            }
            Err(_) => {
                // Channel closed unexpectedly (task panicked)
                let _ = handle.await;
                Err(Error::internal(
                    "TCP service startup task failed unexpectedly",
                ))
            }
        }
    }
}
