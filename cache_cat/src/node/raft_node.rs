use crate::error::{Error, Result};
use crate::node::parsed_config::ParsedConfig;
use crate::raft::application::cluster::Cluster;
use crate::raft::application::connector::Connector;
use crate::raft::application::pub_sub::PubSub;
use crate::raft::network::client::RpcClient;
use crate::raft::network::network::NetworkFactory;
use crate::raft::network::rpc::Server;
use crate::raft::store::log_store::LogStore;
use crate::raft::store::raft_engine::create_raft_engine;
use crate::raft::store::statemachine::StateMachineStore;
use crate::raft::types::entry::membership::JoinRequest;
use crate::raft::types::raft_types::{CacheCatApp, Node, NodeId};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::path::Path;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, oneshot};
use tokio::time::sleep;
use tracing::{debug, error, info};

pub struct RaftNode {
    pub app: Arc<CacheCatApp>,
    shutdown_tx: broadcast::Sender<()>,
    service_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RaftNode {
    pub async fn create(config: ParsedConfig) -> Result<(RaftNode, Receiver<()>)> {
        let node_id = config.node_id as NodeId;
        let dir = Path::new(&config.log_path);
        let path = dir.join("");
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let raft_engine = dir.join("raft-engine");
        let engine = create_raft_engine(raft_engine.clone())?;
        let raft_config = Arc::new(openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: config.election_timeout,
            election_timeout_max: config.election_timeout + 300, // 添加最大选举超时时间
            purge_batch_size: 256,                               //积累到一定一定数量后才进删除
            max_in_snapshot_log_to_keep: config.replication_lag_threshold + 100, //生成快照后要保留的日志数量（以供从节点同步数据）需要大于等于replication_lag_threshold,该参数会影响快照逻辑
            max_append_entries: Some(500000),
            max_payload_entries: 500000,
            snapshot_policy: config.snapshot_policy.clone(), //LogsSinceLast(100),
            replication_lag_threshold: config.replication_lag_threshold, //需要大于snapshot_policy
            install_snapshot_timeout: 60 * 1000,             //60秒
            ..Default::default()
        });
        let group_id = 0;
        let log_store = LogStore::new(group_id, engine.clone());
        let sm_store = StateMachineStore::new(config.clone(), path.clone(), node_id).await?;
        let network = NetworkFactory {};
        let raft = openraft::Raft::new(
            node_id,
            raft_config.clone(),
            network,
            log_store,
            sm_store.clone(),
        )
        .await
        .map_err(|e| Error::internal(format!("Failed to create raft: {}", e)))?;
        let app = CacheCatApp {
            path,
            cluster: Cluster::new(raft, config.raft_advertise_endpoint.clone()),
            config,
            connector: Connector::new(),
            state_machine: sm_store,
            pubsub: Arc::new(PubSub::new()),
            shutdown_tx: shutdown_tx.clone(),
        };

        let node = Self {
            app: Arc::new(app),
            shutdown_tx,
            service_handle: Mutex::new(None),
        };
        Ok((node, shutdown_rx))
    }

    pub async fn start(raft_node: Arc<Self>) -> Result<()> {
        let config = &raft_node.app.config;
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
        let config = &self.app.config;
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
        let config = &self.app.config;
        let addrs = &config.raft_join;
        let mut errors = vec![];
        let raft_address = config.raft_endpoint.raft_addr();
        let raft_advertise_address = config.raft_advertise_endpoint.raft_addr();

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
            self.app.config.node_id,
            addrs,
            errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )))
    }
    async fn join_via(&self, addr: &String) -> Result<()> {
        let config = &self.app.config;

        let join_req = JoinRequest {
            node_id: config.node_id,
            sentinel_master_name: config.sentinel_master_name.clone(),
            endpoint: config.raft_endpoint.clone(),
        };
        // let req = ForwardRequest {
        //     forward_to_leader: 1,
        //     body: ForwardRequestBody::Join(join_req),
        // };
        let client = RpcClient::connect(addr)
            .await
            .map_err(|e| Error::internal(e.to_string()))?;
        let _res: () = client
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
        let app = &self.app;
        if node.node_id != app.cluster.node_id() {
            return Err(Error::config(format!(
                "Node ID {} does not match current node ID {}",
                node.node_id,
                app.cluster.node_id()
            )));
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

        app.cluster.initialize(nodes.clone()).await?;
        Ok(())
    }

    async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
        let config = raft_node.app.config.clone();
        let app = raft_node.app.clone();
        // Subscribe to shutdown signal
        let shutdown_rx = raft_node.shutdown_tx.subscribe();

        // Create oneshot channel to signal startup completion
        let (startup_tx, startup_rx) = oneshot::channel::<StdResult<(), String>>();

        let addr = config.raft_advertise_endpoint.raft_addr();
        let redis_addr = config.raft_advertise_endpoint.redis_addr();
        let handle = tokio::task::spawn(async move {
            // Signal startup success
            let server = match Server::new(app, addr.clone(), startup_tx, redis_addr, &config) {
                Ok(s) => s,
                Err(e) => {
                    // new 内部已经通过 startup_tx 发送了错误信息，外层会正常收到
                    error!("Failed to create TCP server: {e}");
                    return;
                }
            };
            if let Err(e) = server.start_server(shutdown_rx).await {
                error!("Server error: {}", e);
            }
            info!("TCP Service task finished, addr: {}", addr);
        });
        // Wait for startup completion signal
        match startup_rx.await {
            Ok(Ok(())) => {
                // Store the handle in RaftNode
                *raft_node.service_handle.lock() = Some(handle);
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
