use crate::config::config::Config;
use crate::error::{Error, Result};
use crate::node::parsed_config::ParsedConfig;
use crate::protocol::command::CommandFactory;
use crate::raft::network::client::RpcClient;
use crate::raft::network::router::{MultiNetworkFactory, Router};
use crate::raft::network::rpc::Server;
use crate::raft::store::log_store::LogStore;
use crate::raft::store::raft_engine::create_raft_engine;
use crate::raft::store::statemachine::{StateMachineData, StateMachineStore};
use crate::raft::types::entry::forward::{ForwardRequest, ForwardRequestBody};
use crate::raft::types::entry::membership::JoinRequest;
use crate::raft::types::raft_types::{
    App, CacheCatApp, GROUP_NUM, GroupId, Node, NodeId, Raft, TypeConfig,
};
use openraft::SnapshotPolicy::Never;
use openraft::error::{InitializeError, RaftError};
use openraft::raft::ClientWriteResponse;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, oneshot};
use tokio::time::sleep;
use tracing::{debug, error, info};

pub struct RaftNode {
    config: ParsedConfig,

    pub groups: App,

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
        let mut groups = Vec::new();
        let raft_engine = dir.join("raft-engine");
        let engine = create_raft_engine(raft_engine.clone());
        let config = Arc::new(openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 699,
            election_timeout_max: 899, // 添加最大选举超时时间
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
            let app = CacheCatApp {
                node_id,
                addr: app_config.raft.address.clone(),
                raft,
                group_id,
                state_machine: sm_store,
                path: dir.join(""),
            };
            groups.push(app.into());
        }

        let mut node = Self {
            config: ParsedConfig::from(app_config)?,
            groups: Arc::new(groups),
            shutdown_tx,
            _shutdown_rx: shutdown_rx_for_struct,
            service_handle: Mutex::new(None),
        };
        Ok(node)
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
        let config = &self.config;
        if config.raft_join.is_empty() {
            info!("'--join' is empty, do not need joining cluster");
            return Ok(());
        }
        // if self.is_in_cluster()? {
        //     info!("node has already in cluster, do not need joining cluster");
        //     return Ok(());
        // }
        self.do_join_cluster().await?;
        Ok(())
    }

    async fn do_join_cluster(&self) -> Result<()> {
        let config = &self.config;
        let addrs = &config.raft_join;
        let mut errors = vec![];
        let raft_address = config.raft_endpoint.to_string();
        let raft_advertise_address = config.raft_advertise_endpoint.to_string();

        for addr in addrs {
            if addr == &raft_address || addr == &raft_advertise_address {
                debug!("ignore join cluster via self node address {}", addr);
                continue;
            }
            for _i in 0..3 {
                let result = self.join_via(addr).await;
                info!("join cluster via {} result: {:?}", addr, result);

                match result {
                    Ok(x) => return Ok(x),
                    Err(api_error) => {
                        let can_retry = api_error.is_retryable();

                        if can_retry {
                            debug!("try to connect to addr {} again", addr);
                            sleep(Duration::from_millis(1_000)).await;
                            continue;
                        } else {
                            errors.push(api_error);
                            break;
                        }
                    }
                }
            }
        }

        Err(Error::internal(format!(
            "fail to join node-{} to cluster via {:?}, errors: {}",
            self.config.node_id,
            addrs,
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
    async fn join_via(&self, addr: &String) -> Result<()> {
        let config = &self.config;

        let join_req = JoinRequest {
            node_id: config.node_id,
            endpoint: config.raft_endpoint.clone(),
        };
        // let req = ForwardRequest {
        //     forward_to_leader: 1,
        //     body: ForwardRequestBody::Join(join_req),
        // };
        let client = RpcClient::connect(addr)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let res: () = client
            .call(9, join_req)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;

        Ok(())
    }

    /// Initialize the Raft cluster with a single node
    /// * `Ok(())` - Successfully initialized the cluster
    /// * `Err(Error)` - Failed to initialize:
    ///   - `InvalidConfig` if node configuration is invalid
    ///   - `Internal` if adding node to cluster fails
    async fn init_cluster(&self, node: Node) -> Result<()> {
        let groups = &self.groups;
        for app in groups.iter() {
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

        for app in groups.iter() {
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

        let apps: App = raft_node.groups.clone();

        let addr = raft_node.config.raft_advertise_endpoint.to_string();
        let redis_addr = raft_node.config.redis_addr.clone();
        let handle = tokio::task::spawn(async move {
            // Signal startup success
            let server = Server::new(apps, addr, startup_tx, redis_addr);
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
