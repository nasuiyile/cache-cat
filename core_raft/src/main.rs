use core_raft::network::raft::{start_multi_raft, start_multi_raft_app};
use core_raft::server::core::config::{Config, ONE, THREE, TWO, load_config};
use core_raft::store::snapshot_handler::load_cache_from_path;
use mimalloc::MiMalloc;
use openraft::AsyncRuntime;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use std::{env, fs, thread};
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
    // multi_raft()
    start()
}
fn multi_raft() -> Result<(), Box<dyn std::error::Error>> {
    let _base = "/home/suiyi/cache-cat/tmp";
    let base = r"C:/tmp/raft/raft-engine";
    // let base_system = r"C:\zdy\temp\raft-engine";

    // let base_dir = tempfile::tempdir()?;
    // let base_system = base_dir.path();
    // 确保临时目录存在
    fs::create_dir_all(base)?;

    // 在临时目录下创建每个节点的子目录
    let d1 = tempfile::Builder::new()
        .suffix("_1")
        .tempdir_in(base)?
        .keep();

    let d3 = tempfile::Builder::new()
        .suffix("_3")
        .tempdir_in(base)?
        .keep();
    let d2 = tempfile::Builder::new()
        .suffix("_2")
        .tempdir_in(base)?
        .keep();
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(tracing::Level::INFO)
        .init();
    let num_cpus = std::thread::available_parallelism()?.get();

    let _h1 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let _result = rt.block_on(start_multi_raft_app(1, d1, String::from(ONE)));
    });
    sleep(Duration::from_secs(1));

    let _h3 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let _x = rt.block_on(start_multi_raft_app(3, d3, String::from(THREE)));
    });
    sleep(Duration::from_secs(1));

    let _h2 = thread::spawn(move || {
        let rt = Builder::new_multi_thread()
            .max_blocking_threads(512)
            .worker_threads(num_cpus / 3)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        let _x = rt.block_on(start_multi_raft_app(2, d2, String::from(TWO)));
    });
    sleep(Duration::from_secs(40000));
    Ok(())
}
fn start() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let config_path = if args.len() > 2 && args[1] == "--conf" {
        args[2].clone()
    } else {
        eprintln!("Usage: {} --conf <config-file>", args[0]);
        eprintln!("Example: {} --conf conf/node1.toml", args[0]);
        std::process::exit(1);
    };

    let config: Config = load_config(&config_path)?;
    //  创建 runtime 并执行 async
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(start_multi_raft(&config))?;

    Ok(())
}

// async fn raft() -> Result<(), Box<dyn std::error::Error>> {
//     // let base = r"E:\tmp\raft\rocks";
//     let base_dir = tempfile::tempdir()?;
//     let base = base_dir.path();
//     // 确保临时目录存在
//     fs::create_dir_all(base)?;
//
//     // 在临时目录下创建每个节点的子目录
//     let d1 = TempDir::new_in(base)?;
//     let d2 = TempDir::new_in(base)?;
//     let d3 = TempDir::new_in(base)?;
//     // Setup the logger
//     tracing_subscriber::fmt()
//         .with_target(true)
//         .with_thread_ids(true)
//         .with_level(true)
//         .with_ansi(false)
//         .with_env_filter(EnvFilter::from_default_env())
//         .with_max_level(tracing::Level::WARN)
//         .init();
//     let num_cpus = std::thread::available_parallelism()?.get();
//     let _h1 = thread::spawn(move || {
//         let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
//         let x = rt.block_on(network::raft::start_raft_app(
//             1,
//             d1.path(),
//             String::from(ONE),
//         ));
//     });
//     let _h2 = thread::spawn(move || {
//         let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
//         let x = rt.block_on(network::raft::start_raft_app(
//             2,
//             d2.path(),
//             String::from(TWO),
//         ));
//     });
//
//     let _h3 = thread::spawn(move || {
//         let mut rt = AsyncRuntimeOf::<TypeConfig>::new(num_cpus / 2);
//         let x = rt.block_on(network::raft::start_raft_app(
//             3,
//             d3.path(),
//             String::from(THREE),
//         ));
//     });
//
//     tokio::time::sleep(Duration::from_secs(40000)).await;
//     Ok(())
// }
