use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'T', long, default_value = "cachecat")]
    pub target: String,

    #[arg(short = 'm', long, default_value = "throughput")]
    pub mode: String,

    #[arg(short = 'o', long, default_value = "pwrite")]
    pub op: String,

    #[arg(short = 'c', long, default_value_t = 100)]
    pub count: usize,

    #[arg(short = 'n', long, default_value_t = 600)]
    pub clients: usize,

    #[arg(short = 't', long, default_value_t = 10000000)]
    pub total: usize,

    #[arg(short = 'e', long, default_value = "127.0.0.1:5001")]
    pub endpoints: String,

    #[arg(long, default_value_t = false)]
    pub redis: bool,

    #[arg(long, default_value = "127.0.0.1:6379")]
    pub redis_endpoints: String,

    #[arg(short = 'p', long, default_value_t = 100000)]
    pub warmup: usize,
}
