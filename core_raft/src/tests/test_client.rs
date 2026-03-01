use crate::network::model::Request;
use crate::network::node::TypeConfig;
use crate::server::client::client::RpcMultiClient;
use crate::server::handler::model::{PrintTestReq, PrintTestRes, SetReq};
use openraft::raft::ClientWriteResponse;
use std::time::{Duration, Instant};
use tokio::time;

#[tokio::test]
async fn test_add() {
    let client = RpcMultiClient::connect("127.0.0.1:3003")
        .await
        .expect("connect failed");

    const ITERATIONS: u32 = 100;

    // ========================
    // 1️⃣ 测写延迟
    // ========================
    let mut total_write = Duration::ZERO;

    for i in 0..ITERATIONS {
        time::sleep(Duration::from_millis(1)).await;
        let start = Instant::now();
        let _: ClientWriteResponse<TypeConfig> = client
            .call(
                2,
                Request::Set(SetReq {
                    key: format!("test_{}", i).into_bytes(),
                    value: format!("test_value_{}", i).into_bytes(),
                    ex_time: 0,
                }),
            )
            .await
            .expect("write call failed");

        total_write += start.elapsed();
    }

    let avg_write = total_write / ITERATIONS;
    println!("写入平均耗时: {} 微秒", avg_write.as_micros());

    // 等待系统稳定
    time::sleep(Duration::from_secs(1)).await;

    // ========================
    // 2️⃣ 测读 / RPC 延迟
    // ========================
    let mut total_read = Duration::ZERO;

    for i in 0..ITERATIONS {
        let start = Instant::now();

        let _: PrintTestRes = client
            .call(
                1,
                PrintTestReq {
                    message: "xxx".to_string(),
                },
            )
            .await
            .expect("read call failed");

        let elapsed = start.elapsed();
        total_read += elapsed;

        println!("第 {} 次 - {} 微秒", i + 1, elapsed.as_micros());
    }

    let avg_read = total_read / ITERATIONS;
    println!("读/RPC 平均耗时: {} 微秒", avg_read.as_micros());
}
