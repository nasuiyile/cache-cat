use cache_cat::config::config::load_config;
use cache_cat::node::raft_builder::RaftNodeBuilder;
use cache_cat::raft::types::entry::bae_operation::BaseOperation::Set;
use cache_cat::raft::types::entry::bae_operation::SetReq;
use cache_cat::raft::types::entry::request::Request;
use cache_cat::raft::types::raft_types::{CacheCatApp, get_group};
use mimalloc::MiMalloc;
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::signal;
use tokio::time::sleep;
use tracing::info;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //设置日志级别
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Parse command line arguments first to get config path
    let args: Vec<String> = env::args().collect();
    let config_path = if args.len() > 2 && args[1] == "--conf" {
        args[2].clone()
    } else {
        eprintln!("Usage: {} --conf <config-file>", args[0]);
        eprintln!("Example: {} --conf conf/node1.toml", args[0]);
        std::process::exit(1);
    };

    // Load configuration first (without logging)
    let config = load_config(&config_path)?;

    let raft_node = RaftNodeBuilder::build(&config).await?;
    let option = raft_node.groups.get(0).cloned().unwrap();

    // if config.node_id == 1 {
    //     tokio::spawn(async move {
    //         benchmark_requests(option).await;
    //     });
    // }

    // Wait for Ctrl+C
    info!("Press Ctrl+C to shutdown...");
    signal::ctrl_c().await?;

    info!("Shutting down Raft node...");
    // raft_node.shutdown().await?;
    info!("Raft node shutdown successfully");

    info!("Server shutdown complete");
    Ok(())
}
async fn benchmark_requests(apps: Arc<CacheCatApp>) {
    sleep(std::time::Duration::from_secs(5)).await;
    println!("Starting benchmark...");
    let start_time = std::time::Instant::now();
    let mut handles = Vec::new();
    let thread = 500;
    let num: u32 = 50000;
    // 创建 100 个并发任务
    for _ in 0..thread {
        let apps_clone = apps.clone();
        let handle = tokio::spawn(async move {
            for i in 0..num {
                // sleep(std::time::Duration::from_millis(1)).await;
                let request = Request::Base(Set(SetReq {
                    key: Arc::from((num).to_be_bytes().to_vec()),
                    value: Arc::from(Vec::from(format!("value_{}", i))),
                    ex_time: 0,
                }));
                //往第一个group发送请求
                apps_clone.raft.client_write(request).await.unwrap();
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
