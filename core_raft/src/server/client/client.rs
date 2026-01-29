use bincode2;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::sync::atomic::AtomicI32;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct RpcMultiClient {
    clients: Vec<RpcClient>,
    next_client: AtomicI32,
}
impl RpcMultiClient {
    pub async fn connect(addr: &str, count: u32) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut clients = Vec::new();
        for i in 0..count {
            let client = RpcClient::connect(addr).await?;
            clients.push(client);
        }
        Ok(Self {
            clients,
            next_client: AtomicI32::new(0),
        })
    }
    pub async fn call<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
    ) -> Result<Res, Box<dyn std::error::Error + Send + Sync>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        //每次迭代下一个client进行发送
        let client = &self.clients
            [self.next_client.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len()];
        client.call(func_id, req).await
    }
}

/// 异步高并发 RPC 客户端（基于 LengthDelimitedCodec）
 struct RpcClient {
    tx_writer: mpsc::Sender<BytesMut>,
    pending: Arc<DashMap<u32, oneshot::Sender<bytes::Bytes>>>,
    next_request_id: Arc<AtomicU32>,
}

impl RpcClient {
    /// 连接并启动读写后台任务
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?; // 关闭 Nagle，低延迟

        // 使用 LengthDelimitedCodec 自动处理 4-byte 长度前缀
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let (mut sink, mut stream) = framed.split();

        let pending = Arc::new(DashMap::<u32, oneshot::Sender<bytes::Bytes>>::new());
        let pending_reader = pending.clone();

        // 发送队列（带背压），将 WriteRequest 发给写任务，由写任务把帧发出
        let (tx_writer, mut rx_writer) = mpsc::channel::<BytesMut>(1024);

        // 写任务：从 mpsc 接收 payload (BytesMut)，通过 sink 发送（LengthDelimitedCodec 会添加 length）
        tokio::spawn(async move {
            while let Some(req) = rx_writer.recv().await {
                // sink expects BytesMut (LengthDelimitedCodec::Encoder::Item = BytesMut)
                if let Err(e) = sink.send(req.into()).await {
                    eprintln!("RPC writer: 发送帧失败: {}", e);
                    break;
                }
            }
            // 当通道关闭，退出写任务
            tracing::info!("RPC 写任务结束");
        });

        // 读任务：从 stream 中读出每个 frame（payload 已移除 length header）
        tokio::spawn(async move {
            while let Some(frame_res) = stream.next().await {
                match frame_res {
                    Ok(mut frame) => {
                        // frame: BytesMut, 内容为: [request_id(4) | payload...]
                        if frame.len() < 4 {
                            eprintln!("RPC reader: 收到异常帧，长度 < 4");
                            continue;
                        }
                        // 解析 request_id（big-endian）
                        let request_id = {
                            let b0 = frame[0];
                            let b1 = frame[1];
                            let b2 = frame[2];
                            let b3 = frame[3];
                            u32::from_be_bytes([b0, b1, b2, b3])
                        };
                        // 提取 body (去掉 4 字节 request_id)
                        // 这里使用 split_off 保持零拷贝语义（frame 是 BytesMut）
                        let body = frame.split_off(4).freeze(); // bytes::Bytes

                        // 从 pending 中取出对应的 oneshot sender
                        if let Some((_, tx)) = pending_reader.remove(&request_id) {
                            let _ = tx.send(body);
                        } else {
                            // 可能请求已超时或被取消
                            tracing::warn!("RPC reader: 未找到 request_id {}", request_id);
                        }
                    }
                    Err(e) => {
                        eprintln!("RPC reader: 读取帧错误: {}", e);
                        break;
                    }
                }
            }

            // 连接断开：遍历 pending 并通知错误（这里简单清除）
            // 更好的做法是遍历并发送错误给每个 waiting oneshot
            tracing::info!("RPC 读任务结束，清理 pending");
            pending_reader.clear();
        });

        Ok(Self {
            tx_writer,
            pending,
            next_request_id: Arc::new(AtomicU32::new(1)),
        })
    }

    /// 发起 RPC 调用：func_id + req 序列化为 body，返回反序列化后的 Res
    pub async fn call<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
    ) -> Result<Res, Box<dyn std::error::Error + Send + Sync>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        // 生成 request_id
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);

        // 序列化请求体
        let payload_bytes = bincode2::serialize(&req)?; // Vec<u8>

        // 组装 payload: request_id(4) | func_id(4) | payload...
        let frame_len = 4 + 4 + payload_bytes.len(); // 4 for request_id, 4 for func_id
        let mut buf = BytesMut::with_capacity(frame_len);
        buf.put_u32(request_id);
        buf.put_u32(func_id);
        buf.put_slice(&payload_bytes);

        // 注册回调 channel（在发送前注册以避免 race）
        let (tx, rx) = oneshot::channel();
        self.pending.insert(request_id, tx);

        // 发送给写任务（通过 mpsc）
        let write_req = buf;
        if let Err(e) = self.tx_writer.send(write_req).await {
            // 发送失败，可能写任务已退出
            let _ = self.pending.remove(&request_id);
            return Err(format!("RPC connection closed: {}", e).into());
        }

        // 等待响应（调用者可以在外层加超时），这里直接 await
        let start = Instant::now();
        let response_bytes = match rx.await {
            Ok(b) => b,
            Err(_) => {
                // 通常是写/读任务退出或连接断开
                let _ = self.pending.remove(&request_id);
                return Err("RPC wait canceled or connection closed".into());
            }
        };
        tracing::info!("RPC 往返耗时: {} us", start.elapsed().as_micros());

        // 反序列化响应
        let res: Res = bincode2::deserialize(&response_bytes)?;
        Ok(res)
    }
}
