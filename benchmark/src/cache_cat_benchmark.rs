use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use cache_cat::raft::network::client::RpcMultiClient;
use cache_cat::raft::network::model::{GetReq, GetRes};
use cache_cat::raft::network::pipeline_client::PipelineMultiClient;
use cache_cat::raft::types::entry::bae_operation::{BaseOperation, SetReq};
use cache_cat::raft::types::entry::request::{Operation, Request};
use cache_cat::raft::types::raft_types::TypeConfig;
use openraft::raft::ClientWriteResponse;

use crate::args::Args;
use crate::common::{BenchmarkTarget, BenchmarkWorker, run_engine};

#[derive(Clone)]
struct CacheCatTarget {
    client: Arc<RpcMultiClient>,
    pipeline_client: Option<Arc<PipelineMultiClient>>,
}

struct CacheCatWorker {
    client: Arc<RpcMultiClient>,
    pipeline_client: Option<Arc<PipelineMultiClient>>,
}

impl BenchmarkTarget for CacheCatTarget {
    type Worker = CacheCatWorker;

    fn worker(&self, _client_id: usize) -> Self::Worker {
        CacheCatWorker {
            client: Arc::clone(&self.client),
            pipeline_client: self.pipeline_client.clone(),
        }
    }
}

impl BenchmarkWorker for CacheCatWorker {
    fn execute<'a>(
        &'a mut self,
        op_type: &'a str,
        request_id: usize,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            if op_type == "write" {
                let res: Result<ClientWriteResponse<TypeConfig>, _> = self
                    .client
                    .call(
                        2,
                        Request::new(
                            0,
                            0,
                            Operation::Base(BaseOperation::Set(SetReq {
                                key: request_id.to_string().into_bytes().into(),
                                value: Vec::from("xxx").into(),
                                ex_time: 0,
                            })),
                        ),
                    )
                    .await;
                res.is_ok()
            } else if op_type == "pwrite" {
                let pipeline_client = self
                    .pipeline_client
                    .as_ref()
                    .expect("pwrite 模式需要 pipeline client");

                let res: Result<ClientWriteResponse<TypeConfig>, _> = pipeline_client
                    .call(Request::new(
                        0,
                        0,
                        Operation::Base(
                            (BaseOperation::Set(SetReq {
                                key: Bytes::from_owner(format!("test{}", request_id)),
                                value: format!("test_value_{}", request_id).into(),
                                ex_time: 0,
                            })),
                        ),
                    ))
                    .await;
                res.is_ok()
            } else {
                let res: Result<GetRes, _> = self
                    .client
                    .call(
                        3,
                        GetReq {
                            db_number: 0,
                            key: request_id.to_string().into_bytes(),
                        },
                    )
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

    println!(">>> 初始化连接池: {} 个连接 <<<", max_connections);

    let client = Arc::new(
        RpcMultiClient::connect_with_num(&args.endpoints, max_connections)
            .await
            .expect("连接失败，请检查端点是否可用"),
    );

    let pipeline_client = if args.op == "pwrite" {
        Some(Arc::new(
            PipelineMultiClient::connect(&args.endpoints, max_connections)
                .await
                .expect("pipeline 连接失败，请检查端点是否可用"),
        ))
    } else {
        None
    };

    let target = CacheCatTarget {
        client,
        pipeline_client,
    };

    if args.mode == "latency" {
        println!(">>> 延迟测试 - 请求数: {} <<<", args.count);
    } else {
        println!(
            ">>> 吞吐量测试 - {}并发/{}请求 <<<",
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
        "====== 性能测试开始 | Target: {} | Mode: {} | Op: {} ======",
        args.target, args.mode, args.op
    );

    if args.mode == "latency" {
        run_engine(target, 1, args.count, args.op, false).await;
    } else {
        run_engine(target, args.clients, args.total, args.op, false).await;
    }

    Ok(())
}
