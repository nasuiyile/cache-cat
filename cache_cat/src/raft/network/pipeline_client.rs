use bincode2;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use openraft::raft::ClientWriteResponse;
use serde::{Serialize, de::DeserializeOwned};
use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// 按你的服务端实际类型导入
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::TypeConfig;

pub struct PipelineClient {
    tx: mpsc::Sender<(
        Request,
        oneshot::Sender<Result<ClientWriteResponse<TypeConfig>, String>>,
    )>,
}

impl PipelineClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        // 发送协议标识位 2
        stream.write_all(&[2u8]).await?;

        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = framed.split();

        // 请求队列：一个请求对应一个响应
        let (tx, mut rx) = mpsc::channel::<(
            Request,
            oneshot::Sender<Result<ClientWriteResponse<TypeConfig>, String>>,
        )>(1024);

        // 响应回调队列：严格 FIFO，对应服务端按顺序返回的帧
        let (cb_tx, mut cb_rx) =
            mpsc::channel::<oneshot::Sender<Result<ClientWriteResponse<TypeConfig>, String>>>(1024);

        // 写任务：序列化请求并发给服务端
        tokio::spawn(async move {
            while let Some((req, cb)) = rx.recv().await {
                match bincode2::serialize(&req) {
                    Ok(encoded) => {
                        if let Err(e) = writer.send(Bytes::from(encoded)).await {
                            let _ = cb.send(Err(format!("Send failed: {}", e)));
                            break;
                        }

                        // 请求已经成功写入，等待读任务按顺序消费对应响应
                        if cb_tx.send(cb).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = cb.send(Err(format!("Serialize error: {}", e)));
                    }
                }
            }
        });

        // 读任务：按服务端返回顺序消费响应帧
        tokio::spawn(async move {
            while let Some(cb) = cb_rx.recv().await {
                match reader.next().await {
                    Some(Ok(frame_bytes)) => {
                        let res: Result<ClientWriteResponse<TypeConfig>, String> =
                            bincode2::deserialize(frame_bytes.as_ref()).expect("Deserialize error");
                        let _ = cb.send(res);
                    }
                    Some(Err(e)) => {
                        let _ = cb.send(Err(format!("Read failed: {}", e)));
                        break;
                    }
                    None => {
                        let _ = cb.send(Err("Connection closed".to_string()));
                        break;
                    }
                }
            }
        });

        Ok(Self { tx })
    }

    /// 发送一个请求，返回一个响应
    pub async fn call(&self, request: Request) -> Result<ClientWriteResponse<TypeConfig>, String> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send((request, tx))
            .await
            .map_err(|_| "Client channel closed".to_string())?;

        rx.await
            .map_err(|_| "Response channel closed".to_string())?
    }
}
