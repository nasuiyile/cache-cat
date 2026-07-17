use bytes::Bytes;
use cache_cat::config::cli_arg::load_config_with_cli;
use cache_cat::config::config::Config;
use cache_cat::node::raft_builder::RaftNodeBuilder;
use cache_cat::protocol::string::set::SetReq;
use cache_cat::raft::types::entry::bae_operation::BaseOperation::Set;
use cache_cat::raft::types::entry::request::{Operation, Request};
use cache_cat::raft::types::raft_types::CacheCatApp;
use mimalloc::MiMalloc;
use std::error::Error;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::{select, signal, spawn};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::time::LocalTime;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = load_config_with_cli()?;

    tracing_subscriber::fmt()
        .with_timer(LocalTime::rfc_3339())
        .with_max_level(tracing::Level::INFO)
        .init();

    let (raft_node, (shutdown_tx, mut shutdown_rx)) = RaftNodeBuilder::build(&config).await?;
    print_msg(&config);
    // if config.node_id == 1 {
    //     let app_clone = raft_node.app.clone();
    //     tokio::spawn(async move {
    //         benchmark_requests(app_clone).await;
    //     });
    // }
    // Wait for Ctrl+C

    if let Ok(listener) = listen_ctrl_c() {
        spawn(async move {
            let mut shutdown_rx = shutdown_tx.subscribe();
            select! {
                _ = shutdown_rx.recv() => {
                    // do nothing and cancel the listen of ctrl-c
                }

                _ = listener => {
                    info!("Received Ctrl+C");
                    let _ = shutdown_tx.send(());
                }
            }
        });
        info!("Press Ctrl+C to shutdown...");
    } else {
        warn!("Failed to register listener for Ctrl-C")
    }

    let _ = shutdown_rx.recv().await;
    info!("Received shutdown signal");

    info!("Shutting down Raft node...");
    if let Ok(listener) = listen_ctrl_c() {
        info!("NOTE: You can press Ctrl-C to force shutdown");
        select! {
            _ = listener => {
                warn!("Received Ctrl+C, force shutdown the node")
            }

            _ = raft_node.app.cluster.shutdown() => {
                info!("Raft node shutdown successfully")
            }
        }
    } else {
        let _ = raft_node.app.cluster.shutdown().await;
        info!("Raft node shutdown successfully")
    }

    info!("Server shutdown complete");
    Ok(())
}

async fn benchmark_requests(apps: Arc<CacheCatApp>) {
    sleep(std::time::Duration::from_secs(3)).await;
    info!("Starting benchmark...");
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    let thread = 50;
    let num: u32 = 2000;
    // 创建 100 个并发任务
    for _ in 0..thread {
        let apps_clone = apps.clone();
        let handle = tokio::spawn(async move {
            for i in 0..num {
                // sleep(std::time::Duration::from_millis(1)).await;
                let request = Request::new(
                    apps_clone.state_machine.data.kvs.get_write_clock(),
                    0,
                    Operation::Base(Set(SetReq {
                        key: Bytes::from_owner((num).to_be_bytes().to_vec()),
                        value: Bytes::from_owner(format!("value_{}", i)),
                        ex_time: 0,
                    })),
                );
                apps_clone.cluster.client_write(request).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        if let Err(e) = handle.await {
            error!("Task failed: {:?}", e);
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

fn print_msg(config: &Config) {
    println!(
        " ______     ______     ______     __  __     ______     ______     ______     ______  "
    );
    println!(
        "/\\  ___\\   /\\  __ \\   /\\  ___\\   /\\ \\_\\ \\   /\\  ___\\   /\\  ___\\   /\\  __ \\   /\\__  _\\ "
    );
    println!(
        "\\ \\ \\____  \\ \\  __ \\  \\ \\ \\____  \\ \\  __ \\  \\ \\  __\\   \\ \\ \\____  \\ \\  __ \\  \\/_/\\ \\/ "
    );
    println!(
        " \\ \\_____\\  \\ \\_\\ \\_\\  \\ \\_____\\  \\ \\_\\ \\_\\  \\ \\_____\\  \\ \\_____\\  \\ \\_\\ \\_\\    \\ \\_\\ "
    );
    println!(
        "  \\/_____/   \\/_/\\/_/   \\/_____/   \\/_/\\/_/   \\/_____/   \\/_____/   \\/_/\\/_/     \\/_/ "
    );
    println!(
        "                                                                                      "
    );
    println!("Raft Address: {}", config.raft.address);
    println!("Redis Port: {}", config.redis.redis_port);
}

fn listen_ctrl_c() -> std::io::Result<impl Future<Output = Option<()>>> {
    let mut listener = cfg_select! {
        windows => {
            signal::windows::ctrl_c()
        }

        unix => {
            signal::unix::ctrl_c()
        }
    }?;

    Ok(async move { listener.recv().await })
}
