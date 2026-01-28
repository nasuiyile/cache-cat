use core_raft::network::model::Request;
use core_raft::network::raft_rocksdb::TypeConfig;
use core_raft::server::client::client::RpcClient;
use core_raft::server::handler::model::{PrintTestReq, PrintTestRes, SetReq};
use openraft::raft::ClientWriteResponse;
use std::time::{Duration, Instant};
use tokio::time;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    let mut client = RpcClient::connect("127.0.0.1:3003").await.unwrap();

    let mut total_elapsed = Duration::new(0, 0);
    let iterations = 100;

    for i in 0..iterations {
        let start = Instant::now();
        let res: ClientWriteResponse<TypeConfig> = client
            .call(
                2,
                Request::Set(SetReq {
                    key: format!("test_{}", i), // 使用不同键避免覆盖
                    value: Vec::from(format!("test_value_{}", i)),
                    ex_time: 0,
                }),
            )
            .await
            .expect("call failed");
        let elapsed = start.elapsed();
        total_elapsed += elapsed;

        // 可选：打印每次的结果用于调试
        // println!("第{}次 - 毫秒: {}", i + 1, elapsed.as_millis());
    }
    let avg_elapsed = total_elapsed / iterations;

    time::sleep(Duration::from_secs(1));
    for i in 0..iterations {
        time::sleep(Duration::from_millis(2));
        let start = Instant::now();
        let res: PrintTestRes = client
            .call(
                1,
                PrintTestReq {
                    message: String::from("xxx"),
                },
            )
            .await
            .expect("call failed");
        let elapsed = start.elapsed();
        println!("第{}次 - 微秒: {}", i + 1, elapsed.as_micros())
    }
    println!("写入操作平均耗时: {} 微秒", avg_elapsed.as_micros());
    Ok(())
}
