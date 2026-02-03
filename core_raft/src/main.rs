use core_raft::network;
use core_raft::network::raft_rocksdb::TypeConfig;
use mimalloc::MiMalloc;
use openraft::AsyncRuntime;
use openraft::alias::AsyncRuntimeOf;
use std::time::Duration;
use std::{fs, thread};
use tempfile::TempDir;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let base = r"E:\tmp\raft\rocks";
    let base_dir = tempfile::tempdir()?;
    let base = base_dir.path();
    // 确保临时目录存在
    fs::create_dir_all(base)?;

    // 在临时目录下创建每个节点的子目录
    let d1 = TempDir::new_in(base)?;
    let d2 = TempDir::new_in(base)?;
    let d3 = TempDir::new_in(base)?;
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        // .with_max_level(tracing::Level::WARN)
        .init();
    let num_cpus = std::thread::available_parallelism()?.get();
    let _h1 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            1,
            d1.path(),
            String::from("127.0.0.1:3001"),
        ));
    });
    let _h2 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            2,
            d2.path(),
            String::from("127.0.0.1:3002"),
        ));
    });
    let _h3 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            3,
            d3.path(),
            String::from("127.0.0.1:3003"),
        ));
    });

    thread::sleep(Duration::from_secs(20000));
    Ok(())
    // network::raft::start_raft_app(1, String::from("127.0.0.1:3001")).await
}
