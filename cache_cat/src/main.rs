use cache_cat::config::config::load_config;
use cache_cat::node::raft_builder::RaftNodeBuilder;
use std::env;
use std::error::Error;
use tokio::signal;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

    // Wait for Ctrl+C
    info!("Press Ctrl+C to shutdown...");
    signal::ctrl_c().await?;

    info!("Shutting down Raft node...");
    // raft_node.shutdown().await?;
    info!("Raft node shutdown successfully");

    info!("Server shutdown complete");
    Ok(())
}
