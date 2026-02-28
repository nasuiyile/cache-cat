use core_raft::network;
use core_raft::network::node::TypeConfig;
use core_raft::network::raft_rocksdb::start_multi_raft_app;
use core_raft::server::core::config::{ONE, THREE, TWO};
use mimalloc::MiMalloc;
use openraft::AsyncRuntime;
use openraft::alias::AsyncRuntimeOf;
use rand_distr::num_traits::one;
use std::thread::sleep;
use std::time::Duration;
use std::{fs, thread};
use tempfile::TempDir;
use tokio::runtime::Builder;
#[cfg(feature = "flamegraph")]
use tracing_flame::FlushGuard;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "flamegraph")]
fn init_flamegraph(
    path: &str,
) -> Result<FlushGuard<std::io::BufWriter<std::fs::File>>, tracing_flame::Error> {
    use tracing_flame::FlameLayer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (flame_layer, guard) = FlameLayer::with_file(path)?;
    tracing_subscriber::registry().with(flame_layer).init();
    eprintln!("flamegraph profiling enabled, output: {}", path);
    Ok(guard)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "tokio-console")]
    {
        console_subscriber::ConsoleLayer::builder()
            .server_addr(([127, 0, 0, 1], 6669))
            .with_default_env()
            .init();
        eprintln!("tokio-console server started on 127.0.0.1:6669");
    }

    #[cfg(feature = "flamegraph")]
    let _flame_guard = init_flamegraph("./flamegraph.folded")?;
    multi_raft()
}
fn multi_raft() -> Result<(), Box<dyn std::error::Error>> {
    let base = "/home/suiyi/cache-cat/tmp";
    let base = r"E:/tmp/raft/raft-engine";
    // let base_system = r"C:\zdy\temp\raft-engine";

    // let base_dir = tempfile::tempdir()?;
    // let base_system = base_dir.path();
    // 确保临时目录存在
    fs::create_dir_all(base)?;

    // 在临时目录下创建每个节点的子目录
    let d1 = TempDir::new_in(base).unwrap().keep();
    let d2 = TempDir::new_in(base).unwrap().keep();
    let d3 = TempDir::new_in(base).unwrap().keep();
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

    // let rt = Builder::new_multi_thread()
    //     .worker_threads(num_cpus)
    //     .max_blocking_threads(512)
    //     .enable_all()
    //     .build()
    //     .unwrap();
    // rt.block_on(async move {
    //     let t1 = tokio::spawn(network::raft_rocksdb::start_multi_raft_app(
    //         1,
    //         d1,
    //         String::from(ONE),
    //     ));
    //     tokio::time::sleep(Duration::from_secs(1)).await;
    //     let t2 = tokio::spawn(network::raft_rocksdb::start_multi_raft_app(
    //         2,
    //         d2,
    //         String::from(TWO),
    //     ));
    //     tokio::time::sleep(Duration::from_secs(1)).await;
    //     let t3 = tokio::spawn(network::raft_rocksdb::start_multi_raft_app(
    //         3,
    //         d3,
    //         String::from(THREE),
    //     ));
    //     let _ = tokio::join!(t1, t2, t3);
    // });

    let _h1 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let result = rt.block_on(start_multi_raft_app(1, d1, String::from(ONE)));
    });
    let _h2 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let x = rt.block_on(start_multi_raft_app(2, d2, String::from(TWO)));
    });
    let _h3 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let x = rt.block_on(start_multi_raft_app(3, d3, String::from(THREE)));
    });
    sleep(Duration::from_secs(40000));
    Ok(())
}
async fn raft() -> Result<(), Box<dyn std::error::Error>> {
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
    let _h1 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            1,
            d1.path(),
            String::from(ONE),
        ));
    });
    let _h2 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            2,
            d2.path(),
            String::from(TWO),
        ));
    });

    let _h3 = thread::spawn(move || {
        let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
        let x = rt.block_on(network::raft_rocksdb::start_raft_app(
            3,
            d3.path(),
            String::from(THREE),
        ));
    });

    tokio::time::sleep(Duration::from_secs(40000)).await;
    Ok(())
}
