use clap::Parser;
use core_raft::network::model::Request;
use core_raft::network::node::TypeConfig;
use core_raft::server::client::client::RpcMultiClient;
use core_raft::server::handler::model::{PrintTestReq, PrintTestRes, SetReq};
use openraft::raft::ClientWriteResponse;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'T', long, default_value = "cachecat")]
    target: String,

    #[arg(short = 'm', long, default_value = "throughput")]
    mode: String,

    #[arg(short = 'o', long, default_value = "write")]
    op: String,

    #[arg(short = 'c', long, default_value_t = 100)]
    count: usize,

    #[arg(short = 'n', long, default_value_t = 100)]
    clients: usize,

    #[arg(short = 't', long, default_value_t = 100000)]
    total: usize,

    #[arg(short = 'e', long, default_value = "127.0.0.1:3003")]
    endpoints: String,

    #[arg(short = 'h', long, default_value_t = 100000)]
    warmup: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.mode == "latency" {
        println!(">>> 延迟测试 (Latency) - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> 吞吐量测试 (Throughput) - {}并发/{}请求 <<<",
            args.clients, args.total
        );
        println!(">>> 预热阶段 - 发送 {} 个请求 <<<", args.warmup);
        run_engine(
            400,
            args.warmup,
            args.endpoints.clone(),
            args.op.clone(),
            true,
        )
        .await;
        println!(">>> 预热完成，正式测试即将开始 <<<");
    }
    println!(
        "====== 性能测试开始 | Target: {} | Mode: {} | Op: {} ======",
        args.target, args.mode, args.op
    );

    if args.mode == "latency" {
        run_engine(1, args.count, args.endpoints, args.op, false).await;
    } else {
        run_engine(args.clients, args.total, args.endpoints, args.op, false).await;
    }

    Ok(())
}

async fn run_engine(
    client_num: usize,
    total_tasks: usize,
    endpoints: String,
    op_type: String,
    is_warmup: bool,
) {
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));
    let ops = Arc::new(AtomicI64::new(0));
    let start_time = Instant::now();
    let mut handles = vec![];

    // 进度条监控协程
    let ops_clone = Arc::clone(&ops);
    let total_tasks_f = total_tasks as f64;
    if (!is_warmup) {
        tokio::spawn(async move {
            // [修改点] 将定时器设置为 5秒
            // 这会极大减少 I/O 刷新操作，且 await 期间不消耗 CPU
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await; // 异步等待，不占资源

                let curr = ops_clone.load(Ordering::Relaxed);
                if curr >= total_tasks as i64 {
                    break;
                }
                print!(
                    "\r进度: {}/{} ({:.1}%) | 实时TPS: {:.2}",
                    curr,
                    total_tasks,
                    (curr as f64 / total_tasks_f) * 100.0,
                    curr as f64 / start_time.elapsed().as_secs_f64()
                );
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        });
    }

    let tasks_per_client = total_tasks / client_num;

    for cid in 0..client_num {
        let latencies_c = Arc::clone(&latencies);
        let ops_c = Arc::clone(&ops);
        let eps = endpoints.clone();
        let op = op_type.clone();
        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            if let Ok(mut client) = RpcMultiClient::connect_with_num(&eps, 1).await {
                // 预分配本地向量，避免全局锁竞争
                let mut local_latencies = Vec::with_capacity(tasks_per_client);
                for i in 0..tasks_per_client {
                    let start = Instant::now();
                    let success;

                    if op == "write" {
                        let res: Result<ClientWriteResponse<TypeConfig>, _> = client
                            .call(
                                2,
                                Request::Set(SetReq {
                                    key: (&i.to_string()).into(),
                                    value: Vec::from("xxx"),
                                    ex_time: 0,
                                }),
                            )
                            .await;
                        success = res.is_ok();
                    } else {
                        let res: Result<PrintTestRes, _> = client
                            .call(
                                1,
                                PrintTestReq {
                                    message: String::from("xxx"),
                                },
                            )
                            .await;
                        success = res.is_ok();
                    }

                    if success {
                        let duration = start.elapsed();
                        // 写入本地，无锁
                        local_latencies.push(duration);
                        // 原子计数，Relaxed 模式最低开销
                        ops_c.fetch_add(1, Ordering::Relaxed);
                    }

                    if mode == "latency" {
                        sleep(Duration::from_millis(2)).await;
                    }
                }

                // 任务完成后，一次性合并数据，大幅降低 Mutex 争用
                if !local_latencies.is_empty() {
                    let mut global = latencies_c.lock().await;
                    global.extend(local_latencies);
                }
            } else {
                eprintln!("客户端连接失败: {}", eps);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();
    print!("\r"); // 清理进度条行
    let final_latencies = latencies.lock().await;
    if (!is_warmup) {
        print_stats(&final_latencies, elapsed, total_tasks);
    }
}

fn print_stats(d: &[Duration], elapsed: Duration, total_req: usize) {
    if d.is_empty() {
        println!("\n❌ 无成功样本");
        return;
    }

    let mut sorted_d = d.to_vec();
    sorted_d.sort();

    let sum: Duration = sorted_d.iter().sum();
    let avg = sum / sorted_d.len() as u32;

    println!("\n---------------- 测试结果 ----------------");
    println!("完成/总计:    {}/{}", sorted_d.len(), total_req);
    println!("总运行耗时:   {:.2}s", elapsed.as_secs_f64());
    println!(
        "平均吞吐量:   {:.2} req/s",
        sorted_d.len() as f64 / elapsed.as_secs_f64()
    );
    println!("\n--- 延迟分布 (Latency) ---");
    println!("最小值 (Min): {:?}", sorted_d[0]);
    println!("平均值 (Avg): {:?}", avg);
    println!(
        "中位数 (P50): {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.5) as usize]
    );
    println!(
        "九九线 (P99): {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.99) as usize]
    );
    println!("最大值 (Max): {:?}", sorted_d[sorted_d.len() - 1]);
    println!("------------------------------------------");
}
