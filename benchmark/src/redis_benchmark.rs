use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redis::aio::MultiplexedConnection;
use redis::RedisResult;

use crate::args::Args;
use crate::common::{BenchmarkTarget, BenchmarkWorker, run_engine};

#[derive(Clone)]
struct RedisTarget {
    conns: Arc<Vec<MultiplexedConnection>>,
}

struct RedisWorker {
    conn: MultiplexedConnection,
}

impl BenchmarkTarget for RedisTarget {
    type Worker = RedisWorker;

    fn worker(&self, client_id: usize) -> Self::Worker {
        RedisWorker {
            conn: self.conns[client_id % self.conns.len()].clone(),
        }
    }
}

impl BenchmarkWorker for RedisWorker {
    fn execute<'a>(
        &'a mut self,
        op_type: &'a str,
        request_id: usize,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            if op_type == "write" || op_type == "pwrite" {
                let key = if op_type == "pwrite" {
                    format!("test{}", request_id)
                } else {
                    request_id.to_string()
                };
                let value = if op_type == "pwrite" {
                    format!("test_value_{}", request_id)
                } else {
                    String::from("xxx")
                };

                let res: RedisResult<()> = redis::cmd("SET")
                    .arg(&key)
                    .arg(&value)
                    .query_async(&mut self.conn)
                    .await;
                res.is_ok()
            } else {
                let key = request_id.to_string();
                let res: RedisResult<Option<Vec<u8>>> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut self.conn)
                    .await;
                res.is_ok()
            }
        })
    }
}

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let max_connections = if args.mode == "latency" {
        1
    } else {
        args.clients
    };

    println!(">>> 初始化 Redis 连接池: {} 个连接 <<<", max_connections);

    let conns = Arc::new(
        connect_redis_with_num(&args.redis_endpoints, max_connections)
            .await
            .expect("Redis 连接失败，请检查端点是否可用"),
    );

    let target = RedisTarget { conns };

    if args.mode == "latency" {
        println!(">>> Redis 延迟测试 - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> Redis 吞吐量测试 - {}并发/{}请求 <<<",
            args.clients, args.total
        );
        println!(">>> 预热阶段 - 发送 {} 个请求 <<<", args.warmup);

        run_engine(
            target.clone(),
            args.clients,
            args.warmup,
            args.op.clone(),
            true,
        )
        .await;

        println!(">>> 预热完成，正式测试即将开始 <<<");
    }
    println!(
        "====== 性能测试开始 | Target: redis | Endpoints: {} | Mode: {} | Op: {} ======",
        args.redis_endpoints, args.mode, args.op
    );

    if args.mode == "latency" {
        run_engine(target, 1, args.count, args.op, false).await;
    } else {
        run_engine(target, args.clients, args.total, args.op, false).await;
    }

    Ok(())
}

async fn connect_redis_with_num(
    endpoints: &str,
    num: usize,
) -> RedisResult<Vec<MultiplexedConnection>> {
    let urls = parse_redis_endpoints(endpoints);
    let mut conns = Vec::with_capacity(num);

    for i in 0..num {
        let client = redis::Client::open(urls[i % urls.len()].as_str())?;
        conns.push(client.get_multiplexed_async_connection().await?);
    }

    Ok(conns)
}

fn parse_redis_endpoints(endpoints: &str) -> Vec<String> {
    let urls: Vec<String> = endpoints
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(normalize_redis_endpoint)
        .collect();

    if urls.is_empty() {
        vec![normalize_redis_endpoint("127.0.0.1:6379")]
    } else {
        urls
    }
}

fn normalize_redis_endpoint(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    if endpoint.starts_with("redis://") || endpoint.starts_with("rediss://") {
        endpoint.to_string()
    } else {
        format!("redis://{}/", endpoint.trim_end_matches('/'))
    }
}
