use crate::network::model::{BaseOperation, Request};
use crate::network::node::{App, CacheCatApp, NodeId, create_node};
use crate::protocol::command::CommandFactory;
use crate::server::core::config::{Config, ONE, THREE, TWO};
use crate::server::handler::model::SetReq;
use crate::server::handler::rpc::Server;
use openraft::BasicNode;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

// pub async fn start_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
// where
//     P: AsRef<Path>,
// {
//     let config = Arc::new(Config {
//         heartbeat_interval: 250,
//         election_timeout_min: 299,
//         election_timeout_max: 599, // 添加最大选举超时时间
//         ..Default::default()
//     });
//     let path = dir.as_ref().join("raft-engine");
//
//     let raft_engine = dir.as_ref().join("raft-engine");
//     // let rocksdb_path = dir.as_ref().join("rocksdb");
//     let engine = create_raft_engine(raft_engine.clone());
//     // let db: Arc<DB> = new_storage(rocksdb_path).await;
//     let log_store = LogStore::new(0, engine.clone());
//     let sm_store = StateMachineStore::new(path, 0).await?;
//     let network = NetworkFactory {};
//
//     let raft = openraft::Raft::new(
//         node_id,
//         config.clone(),
//         network,
//         log_store,
//         sm_store.clone(),
//     )
//     .await
//     .unwrap();
//
//     let app = CacheCatApp {
//         id: node_id,
//         addr: addr.clone(),
//         raft,
//         group_id: 0,
//         state_machine: sm_store,
//         path: dir.as_ref().join(""),
//     };
//
//     // 正确构建集群成员映射
//     let mut nodes = BTreeMap::new();
//     if node_id == 3 {
//         nodes.insert(
//             1,
//             BasicNode {
//                 addr: ONE.to_string(),
//             },
//         );
//         nodes.insert(
//             2,
//             BasicNode {
//                 addr: TWO.to_string(),
//             },
//         );
//         nodes.insert(
//             3,
//             BasicNode {
//                 addr: THREE.to_string(),
//             },
//         );
//         app.raft.initialize(nodes).await.unwrap();
//     }
//     // 根据node_id决定完整的集群配置
//
//     rpc::start_server(App::new(vec![Arc::new(app)]), addr).await
// }
pub async fn start_multi_raft_app<P>(node_id: NodeId, dir: P, addr: String) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    let node = create_node(&addr, node_id, dir).await;
    let apps: Vec<Arc<CacheCatApp>> = node.groups.into_values().map(Arc::new).collect();
    let mut nodes = HashMap::new();
    let redis_addr = if node_id == 3 {
        nodes.insert(
            1,
            BasicNode {
                addr: ONE.to_string(),
            },
        );
        nodes.insert(
            2,
            BasicNode {
                addr: TWO.to_string(),
            },
        );
        nodes.insert(
            3,
            BasicNode {
                addr: THREE.to_string(),
            },
        );
        for app in &apps {
            app.raft.initialize(nodes.clone()).await.unwrap();
        }
        let apps_for_task = apps.clone();

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            benchmark_requests(apps_for_task).await;
        });
        "127.0.0.1:6379"
    } else if node_id == 2 {
        // tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        "127.0.0.1:6378"
    } else {
        // tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        "127.0.0.1:6377"
    };
    // Initialize command factory
    let cmd_factory = Arc::new(CommandFactory::init());
    let server = Server {
        app: App::new(apps),
        addr,
        cmd_factory,
        redis_addr: redis_addr.to_string(),
    };
    let redis_server = server.clone();
    tokio::spawn(async move {
        Arc::new(redis_server)
            .clone()
            .start_redis_server()
            .await
            .expect("Redis : panic message");
    });
    server.start_server().await?;
    Ok(())
}
pub async fn start_multi_raft(config: &Config) -> std::io::Result<()> {
    let node = create_node(
        &config.raft.address,
        config.node_id,
        Path::new(&config.raft.log_path),
    )
    .await;

    let apps: Vec<Arc<CacheCatApp>> = node.groups.into_values().map(Arc::new).collect();
    let mut nodes = BTreeMap::new();
    if config.raft.single {
        nodes.insert(
            1,
            BasicNode {
                addr: config.raft.address.clone(),
            },
        );
        for app in &apps {
            app.raft.initialize(nodes.clone()).await.unwrap();
        }
    }
    // Initialize command factory
    let cmd_factory = Arc::new(CommandFactory::init());
    let server = Server {
        app: App::new(apps),
        addr: config.raft.address.clone(),
        cmd_factory,
        redis_addr: config.redis_address.to_string(),
    };
    let redis_server = server.clone();
    tokio::spawn(async move {
        Arc::new(redis_server)
            .clone()
            .start_redis_server()
            .await
            .expect("Redis : panic message");
    });
    server.start_server().await?;
    Ok(())
}

//这个方法用于测试主节点直接迭代状态机
async fn benchmark_requests(apps: Vec<Arc<CacheCatApp>>) {
    println!("Starting benchmark...");
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    let thread = 1;
    let num: u32 = 5000;
    // 创建 100 个并发任务
    for _ in 0..thread {
        let apps_clone = apps.clone();
        let handle = tokio::spawn(async move {
            for i in 0..num {
                // sleep(std::time::Duration::from_millis(1)).await;
                let request = Request::Base(BaseOperation::Set(SetReq {
                    key: Arc::from((num).to_be_bytes().to_vec()),
                    value: Arc::from(Vec::from(format!("value_{}", i))),
                    ex_time: 0,
                }));
                //往第一个group发送请求
                if let Some(app) = apps_clone.get(0) {
                    match app.raft.client_write(request).await {
                        Ok(_) => (),
                        Err(e) => eprintln!("Raft write {} failed: {:?}", i, e),
                    }
                }
            }
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Task failed: {:?}", e);
        }
    }

    let elapsed = start_time.elapsed();
    let total_requests = thread * num;
    let rps = total_requests as f64 / elapsed.as_secs_f64();

    println!("=========================================");
    println!("Benchmark Results:");
    println!("Total requests: {}", total_requests);
    println!("Elapsed time: {:.2?}", elapsed);
    println!("Throughput: {:.2} requests/second", rps);
    println!(
        "Average latency: {:.3} ms",
        elapsed.as_millis() as f64 / total_requests as f64
    );
    println!("=========================================");
}
