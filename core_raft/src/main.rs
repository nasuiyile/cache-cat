use core_raft::error::CoreRaftResult;
use core_raft::network;
use core_raft::network::raft_rocksdb::TypeConfig;
use mimalloc::MiMalloc;
use openraft::AsyncRuntime;
use openraft::alias::AsyncRuntimeOf;
use std::time::Duration;
use std::{fs, thread};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> CoreRaftResult<()> {
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
        .with_max_level(tracing::Level::WARN)
        .init();

    let num_cpus = std::thread::available_parallelism()?.get();
    let ((tx1, rx1), (tx2, rx2), (tx3, rx3)) = (
        oneshot::channel::<()>(),
        oneshot::channel::<()>(),
        oneshot::channel::<()>(),
    );
    let h1 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let _ = rt.block_on(async move {
            network::raft_rocksdb::start_raft_app(
                1,
                d1.path(),
                String::from("127.0.0.1:3001"),
                tx1,
            )
            .await
            .unwrap();
        });
    });
    let h2 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let _ = rt.block_on(async move {
            let _ = rx1.await.unwrap();
            network::raft_rocksdb::start_raft_app(
                2,
                d2.path(),
                String::from("127.0.0.1:3002"),
                tx2,
            )
            .await
            .unwrap();
        });
    });
    let h3 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus);
        let _ = rt.block_on(async move {
            let _ = rx2.await.unwrap();
            network::raft_rocksdb::start_raft_app(
                3,
                d3.path(),
                String::from("127.0.0.1:3003"),
                tx3,
            )
            .await
            .unwrap();
        });
    });

    [h1, h2, h3].into_iter().for_each(|h| h.join().unwrap());
    let _ = rx3.await;
    //thread::sleep(Duration::from_secs(20000));

    Ok(())
    // network::raft::start_raft_app(1, String::from("127.0.0.1:3001")).await
}
