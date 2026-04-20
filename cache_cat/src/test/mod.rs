#[cfg(test)]
mod tests {

    use crate::raft::network::client::RpcMultiClient;
    use crate::raft::network::model::{GetReq, GetRes, PrintTestReq, PrintTestRes};
    use crate::raft::types::entry::bae_operation::{BaseOperation, SetReq};
    use crate::raft::types::entry::request::Request;
    use crate::raft::types::raft_types::TypeConfig;
    use openraft::RPCTypes::Vote;
    use openraft::error::Timeout;
    use openraft::raft::ClientWriteResponse;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time;

    #[tokio::test]
    async fn test_add() {
        let client = RpcMultiClient::connect("127.0.0.1:5001")
            .await
            .expect("connect failed");

        const ITERATIONS: u32 = 200;

        // // ========================
        // // 1️⃣ 测写延迟
        // // ========================
        let mut total_write = Duration::ZERO;

        for i in 0..ITERATIONS {
            time::sleep(Duration::from_millis(1)).await;
            let start = Instant::now();
            let r: ClientWriteResponse<TypeConfig> = client
                .call(
                    2,
                    Request::Base(BaseOperation::Set(SetReq {
                        key: Arc::from(format!("test_{}", i).into_bytes()),
                        value: Arc::from(format!("test_value_{}", i).into_bytes()),
                        ex_time: 0,
                    })),
                )
                .await
                .expect("write call failed");
            let r: GetRes = client
                .call(
                    3,
                    GetReq {
                        key: Vec::from("xxx"),
                    },
                )
                .await
                .expect("read call failed");

            total_write += start.elapsed();
        }

        // let l_push_res: ClientWriteResponse<TypeConfig> = client
        //     .call(
        //         2,
        //         Request::Base(BaseOperation::LPush(LPushReq {
        //             key: Arc::from(format!("tes1t_{}", 1).into_bytes()),
        //             value: Arc::from(format!("test_value_{}", 1).into_bytes()),
        //         })),
        //     )
        //     .await
        //     .expect("write call failed");

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

            let res: PrintTestRes = match client
                .call_with_timeout(
                    1,
                    PrintTestReq {
                        message: "xxx".to_string(),
                    },
                    Duration::from_secs(3),
                    Timeout {
                        action: Vote,
                        target: 1,
                        timeout: Duration::from_secs(3),
                        id: 1,
                    },
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    println!("Error: {:?}", e);
                    panic!("call failed");
                }
            };

            let elapsed = start.elapsed();
            total_read += elapsed;

            println!("第 {} 次 - {} 微秒", i + 1, elapsed.as_micros());
        }

        let avg_read = total_read / ITERATIONS;
        println!("读/RPC 平均耗时: {} 微秒", avg_read.as_micros());
    }
}
