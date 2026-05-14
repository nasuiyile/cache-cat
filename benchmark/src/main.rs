use clap::Parser;

mod args;
mod cache_cat_benchmark;
mod common;
mod redis_benchmark;

use args::Args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.redis {
        redis_benchmark::run(args).await
    } else {
        cache_cat_benchmark::run(args).await
    }
}
