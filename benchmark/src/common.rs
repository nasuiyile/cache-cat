use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::time::sleep;

pub trait BenchmarkTarget: Clone {
    type Worker: BenchmarkWorker;

    fn worker(&self, client_id: usize) -> Self::Worker;
}

pub trait BenchmarkWorker: Send + 'static {
    fn execute<'a>(
        &'a mut self,
        op_type: &'a str,
        request_id: usize,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

pub async fn run_engine<T>(
    target: T,
    client_num: usize,
    total_tasks: usize,
    op_type: String,
    is_warmup: bool,
) where
    T: BenchmarkTarget,
{
    // 仅用于最终报告的详细数据（为了不影响实时性能，我们只在工作线程内部维护，最后合并）
    let final_latencies = Arc::new(Mutex::new(Vec::with_capacity(total_tasks)));

    // 实时统计用的原子变量 (无锁，极低开销)
    let ops = Arc::new(AtomicI64::new(0));
    let total_dur_us = Arc::new(AtomicU64::new(0)); // 存储总微秒数

    let start_time = Instant::now();
    let mut handles = vec![];

    // 进度条监控协程
    let ops_clone = Arc::clone(&ops);
    let dur_clone = Arc::clone(&total_dur_us);
    let total_tasks_f = total_tasks as f64;

    if !is_warmup {
        let progress_start_time = start_time;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let curr = ops_clone.load(Ordering::Relaxed);
                if curr >= total_tasks as i64 {
                    break;
                }

                let curr_ops = curr as u64;
                let curr_dur_us = dur_clone.load(Ordering::Relaxed);

                let avg_str = if curr_ops > 0 {
                    format!("{:?}", Duration::from_micros(curr_dur_us / curr_ops))
                } else {
                    "N/A".to_string()
                };

                print!(
                    "\r进度: {}/{} ({:.1}%) | 实时TPS: {:.2} | 平均延迟: {}",
                    curr,
                    total_tasks,
                    (curr as f64 / total_tasks_f) * 100.0,
                    curr as f64 / progress_start_time.elapsed().as_secs_f64(),
                    avg_str
                );
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        });
    }

    let tasks_per_client = total_tasks / client_num;

    for cid in 0..client_num {
        let latencies_c = Arc::clone(&final_latencies);
        let ops_c = Arc::clone(&ops);
        let dur_c = Arc::clone(&total_dur_us);
        let mut worker = target.worker(cid);

        let op = op_type.clone();
        let mode = if client_num == 1 {
            "latency"
        } else {
            "throughput"
        };

        let handle = tokio::spawn(async move {
            let mut local_latencies = Vec::with_capacity(tasks_per_client);

            for i in 0..tasks_per_client {
                let start = Instant::now();
                let success = worker.execute(&op, i).await;

                if success {
                    let duration = start.elapsed();

                    // 累加微秒数，用于实时平均计算
                    dur_c.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
                    ops_c.fetch_add(1, Ordering::Relaxed);

                    // 仍然存入本地数组，用于最终报告的 P99 计算
                    local_latencies.push(duration);

                    if local_latencies.len() >= 1000 {
                        let mut global = latencies_c.lock().await;
                        global.extend(local_latencies.drain(..));
                    }
                }

                if mode == "latency" {
                    sleep(Duration::from_millis(2)).await;
                }
            }

            // 剩余数据刷入
            if !local_latencies.is_empty() {
                let mut global = latencies_c.lock().await;
                global.extend(local_latencies);
            }
        });
        handles.push(handle);
    }

    drop(target);

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();
    print!("\r"); // 清除当前行的进度显示

    if !is_warmup {
        let final_lats = final_latencies.lock().await;
        print_stats(&final_lats, elapsed, total_tasks);
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
