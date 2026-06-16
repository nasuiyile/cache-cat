use bytes::Bytes;
use cache_cat::config::cli_arg::load_config_with_cli;
use cache_cat::config::config::Config;
use cache_cat::node::raft_builder::RaftNodeBuilder;
use cache_cat::raft::types::entry::bae_operation::BaseOperation::Set;
use cache_cat::raft::types::entry::bae_operation::SetReq;
use cache_cat::raft::types::entry::request::{Operation, Request};
use cache_cat::raft::types::raft_types::CacheCatApp;
use mimalloc::MiMalloc;
use std::error::Error;
use std::sync::Arc;
use tokio::signal;
use tokio::time::sleep;
use tracing::{error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = load_config_with_cli()?;

    //设置日志级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let (_raft_node, mut shutdown_rx) = RaftNodeBuilder::build(&config).await?;
    // config.redis.redis_port;
    print_msg(&config);
    // if config.node_id == 1 {
    //     let app_clone = raft_node.app.clone();
    //     tokio::spawn(async move {
    //         benchmark_requests(app_clone).await;
    //     });
    // }
    // Wait for Ctrl+C

    info!("Press Ctrl+C to shutdown...");

    tokio::select! {
         _ = signal::ctrl_c() => {
            info!("Received Ctrl+C");
        }
         _ = shutdown_rx.recv()=>{
            info!("Received shutdown signal");
        }
    }

    info!("Shutting down Raft node...");
    // raft_node.shutdown().await?;
    info!("Raft node shutdown successfully");

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
                        value: Arc::from(Vec::from(format!("value_{}", i))),
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
