use clap::Parser;
use core_raft::network::model::Request;
use core_raft::network::raft_rocksdb::TypeConfig;
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
    #[arg(short, long, default_value = "cachecat")]
    target: String,

    #[arg(short, long, default_value = "latency")]
    mode: String,

    #[arg(short, long, default_value = "write")]
    op: String,

    #[arg(short, long, default_value_t = 100)]
    count: usize,

    #[arg(short, long, default_value_t = 10)]
    clients: usize,

    #[arg(short, long, default_value_t = 1000)]
    total: usize,

    #[arg(short, long, default_value = "127.0.0.1:3003")]
    endpoints: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.mode == "latency" {
        println!(">>> 延迟测试 (Latency) - 请求数: {} <<<", args.count);
        run_engine(1, args.count, args.endpoints, args.op).await;
    } else {
        println!(
            ">>> 吞吐量测试 (Throughput) - {}并发协程 / 共用连接池 / {}总请求 <<<",
            args.clients, args.total
        );
        run_engine(args.clients, args.total, args.endpoints, args.op).await;
    }

    Ok(())
}

async fn run_engine(client_num: usize, total_tasks: usize, endpoints: String, op_type: String) {
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));
    let ops = Arc::new(AtomicI64::new(0));
    let start_time = Instant::now();
    let mut handles = vec![];

    // --- 核心修改：在循环外创建唯一的共用客户端 ---
    // 无论 client_num 是多少，底层 TCP 连接数始终固定为 100
    println!("正在建立共用连接池 (100个TCP连接)...");
    let shared_client = match RpcMultiClient::connect(&endpoints, 100).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("无法连接到服务器: {}", e);
            return;
        }
    };

    // 进度条监控协程
    let ops_clone = Arc::clone(&ops);
    let total_tasks_f = total_tasks as f64;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let curr = ops_clone.load(Ordering::Relaxed);
            if curr >= total_tasks as i64 {
                break;
            }

            let elapsed = start_time.elapsed().as_secs_f64();
            let tps = if elapsed > 0.0 {
                curr as f64 / elapsed
            } else {
                0.0
            };

            print!(
                "\r进度: {}/{} ({:.1}%) | 实时TPS: {:.2}",
                curr,
                total_tasks,
                (curr as f64 / total_tasks_f) * 100.0,
                tps
            );
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    });

    // 计算每个协程的任务数，并处理余数确保总数正确
    let base_tasks = total_tasks / client_num;
    let remainder = total_tasks % client_num;

    for cid in 0..client_num {
        let latencies_c = Arc::clone(&latencies);
        let ops_c = Arc::clone(&ops);
        let op = op_type.clone();
        let client = Arc::clone(&shared_client); // 克隆 Arc 引用，不产生新连接

        // 分配任务：第一个协程处理余数
        let tasks_to_run = if cid == 0 {
            base_tasks + remainder
        } else {
            base_tasks
        };

        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            let mut local_latencies = Vec::with_capacity(tasks_to_run);

            for i in 0..tasks_to_run {
                let start = Instant::now();
                let success;

                if op == "write" {
                    let res: Result<ClientWriteResponse<TypeConfig>, _> = client
                        .call(
                            2,
                            Request::Set(SetReq {
                                key: format!("cc_{}_{}", cid, i),
                                value: Vec::from(format!("val_{}", i)),
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
                    local_latencies.push(start.elapsed());
                    ops_c.fetch_add(1, Ordering::Relaxed);
                }

                if mode == "latency" {
                    sleep(Duration::from_millis(2)).await;
                }
            }

            // 任务完成后合并数据
            if !local_latencies.is_empty() {
                let mut global = latencies_c.lock().await;
                global.extend(local_latencies);
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
    print_stats(&final_latencies, elapsed, total_tasks);
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
    let len = sorted_d.len();

    println!("\n---------------- 测试结果 ----------------");
    println!("完成/总计:     {}/{}", len, total_req);
    println!("总运行耗时:    {:.2}s", elapsed.as_secs_f64());
    println!(
        "平均吞吐量:    {:.2} req/s",
        len as f64 / elapsed.as_secs_f64()
    );
    println!("\n--- 延迟分布 (Latency) ---");
    println!("最小值 (Min): {:?}", sorted_d[0]);
    println!("平均值 (Avg): {:?}", avg);
    println!("P50: {:?}", sorted_d[(len as f64 * 0.5) as usize]);
    println!(
        "P99: {:?}",
        sorted_d[((len as f64 * 0.99) as usize).min(len - 1)] // 防止越界
    );
    println!("最大值 (Max): {:?}", sorted_d[len - 1]);
    println!("------------------------------------------");
}
