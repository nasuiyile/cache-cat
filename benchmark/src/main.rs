use core_raft::network::model::Request;
use core_raft::network::raft_rocksdb::TypeConfig;
use core_raft::server::client::client::RpcMultiClient;
use core_raft::server::handler::model::{PrintTestReq, PrintTestRes, SetReq};
use openraft::raft::ClientWriteResponse;
use std::time::{Duration, Instant};
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RpcMultiClient::connect("127.0.0.1:3003", 10).await.unwrap();
    let iterations = 1000;
    // ---------------- 读取操作 ----------------
    time::sleep(Duration::from_secs(1)); // 稍作延迟
    let mut total_read_elapsed = Duration::new(0, 0);
    for i in 0..iterations {
        time::sleep(Duration::from_millis(2));
        let start = Instant::now();
        let _res: PrintTestRes = client
            .call(
                1,
                PrintTestReq {
                    message: String::from("xxx"),
                },
            )
            .await
            .expect("read call failed");
        let elapsed = start.elapsed();
        total_read_elapsed += elapsed;
        println!("第{}次读取 - 微秒: {}", i + 1, elapsed.as_micros());
    }
    let avg_read_elapsed = total_read_elapsed / iterations;

    // ---------------- 写入操作 ----------------
    let mut total_write_elapsed = Duration::new(0, 0);
    for i in 0..iterations {
        let start = Instant::now();
        let _res: ClientWriteResponse<TypeConfig> = client
            .call(
                2,
                Request::Set(SetReq {
                    key: format!("test_{}", i), // 使用不同键避免覆盖
                    value: Vec::from(format!("test_value_{}", i)),
                    ex_time: 0,
                }),
            )
            .await
            .expect("write call failed");
        let elapsed = start.elapsed();
        total_write_elapsed += elapsed;
    }
    let avg_write_elapsed = total_write_elapsed / iterations;

    // ---------------- 输出平均耗时 ----------------
    println!("写入操作平均耗时: {} 微秒", avg_write_elapsed.as_micros());
    println!("读取操作平均耗时: {} 微秒", avg_read_elapsed.as_micros());

    Ok(())
}
