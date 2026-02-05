use crate::network::model::WriteReq;
use crate::network::raft_rocksdb::TypeConfig;
use crate::server::client::client::RpcMultiClient;
use crate::server::handler::model::{PrintTestReq, PrintTestRes, SetReq};
use openraft::raft::ClientWriteResponse;
use std::time::{Duration, Instant};
use tokio::time;

#[tokio::test]
async fn test_add() {
    let mut client = RpcMultiClient::connect("127.0.0.1:3003", 10).await.unwrap();

    let mut total_elapsed = Duration::new(0, 0);
    let iterations = 100;

    for i in 0..100 {
        let start = Instant::now();
        let res: ClientWriteResponse<TypeConfig> = client
            .call(
                2,
                WriteReq::Set(SetReq {
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

    // 验证读取操作
    // let start = Instant::now();
    // let res: Option<String> = client
    //     .call(3, "test_0".to_string()) // 读取第一个插入的值
    //     .await
    //     .expect("call failed");
    // println!("读取结果: {:?}", res.expect("res is none"));
    // let elapsed = start.elapsed();
    // println!("读取耗时: {} 微秒", elapsed.as_micros());
}
