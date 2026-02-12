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

    // 1. 计算所需的最大连接数
    // 吞吐量模式需要 max(预热并发, 测试并发)
    // 延迟模式只需要 1 个连接
    let max_connections = if args.mode == "latency" {
        1
    } else {
        args.clients
    };

    println!(">>> 初始化连接池: {} 个连接 <<<", max_connections);

    // 2. 【核心修改】提前初始化客户端，生命周期提升至 main
    // 使用 Arc 包装，以便在多个协程中共享所有权
    let client = Arc::new(
        RpcMultiClient::connect_with_num(&args.endpoints, max_connections)
            .await
            .expect("连接失败，请检查端点是否可用"),
    );

    if args.mode == "latency" {
        println!(">>> 延迟测试 - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> 吞吐量测试 - {}并发/{}请求 <<<",
            args.clients, args.total
        );
        println!(">>> 预热阶段 - 发送 {} 个请求 <<<", args.warmup);

        // 3. 预热阶段复用连接
        run_engine(
            Arc::clone(&client), // 传递 Arc 克隆
            args.clients,
            args.warmup,
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
        run_engine(Arc::clone(&client), 1, args.count, args.op, false).await;
    } else {
        run_engine(
            Arc::clone(&client),
            args.clients,
            args.total,
            args.op,
            false,
        )
        .await;
    }

    Ok(())
}

// 修改签名：接收 Arc<RpcMultiClient> 而不是 endpoints 字符串
async fn run_engine(
    client: Arc<RpcMultiClient>,
    client_num: usize,
    total_tasks: usize,
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
    if !is_warmup {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
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
        let ops_c: Arc<AtomicI64> = Arc::clone(&ops);

        // 4. 【核心修改】直接克隆 Arc，无需重新建立 TCP 连接
        // 每个任务持有客户端的一个引用，底层共享同一个连接池
        let client_c = Arc::clone(&client);

        let op = op_type.clone();
        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            // 移除了原来的 connect 逻辑
            // 预分配本地向量
            let mut local_latencies = Vec::with_capacity(tasks_per_client);

            for i in 0..tasks_per_client {
                let start = Instant::now();
                let success;

                // 使用已建立的连接发送请求
                if op == "write" {
                    let res: Result<ClientWriteResponse<TypeConfig>, _> = client_c
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
                    let res: Result<PrintTestRes, _> = client_c
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
                    local_latencies.push(duration);
                    ops_c.fetch_add(1, Ordering::Relaxed);
                }

                if mode == "latency" {
                    sleep(Duration::from_millis(2)).await;
                }
            }

            // 合并数据
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
    print!("\r");
    let final_latencies = latencies.lock().await;
    if !is_warmup {
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
    println!("\n--- 延迟分布 ---");
    println!("最小值: {:?}", sorted_d[0]);
    println!("平均值: {:?}", avg);
    println!(
        "中位数: {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.5) as usize]
    );
    println!(
        "P99:    {:?}",
        sorted_d[(sorted_d.len() as f64 * 0.99) as usize]
    );
    println!("最大值: {:?}", sorted_d[sorted_d.len() - 1]);
    println!("------------------------------------------");
}
