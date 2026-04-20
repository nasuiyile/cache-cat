use bincode2;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::error::Error;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::io::AsyncWriteExt;

// 这里的类型根据你的服务端定义进行匹配
// Req -> Request
// Res -> WriteResult<TypeConfig>
pub struct PipelineClient<Req, Res> {
    tx: mpsc::Sender<(Vec<Req>, oneshot::Sender<Result<Vec<Result<Res, String>>, String>>)>,
}

impl<Req, Res> PipelineClient<Req, Res>
where
    Req: Serialize + Send + 'static,
    Res: DeserializeOwned + Send + 'static,
{
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        // 发送协议标识位 2
        stream.write_all(&[2u8]).await?;

        let (mut sink, mut stream) = Framed::new(stream, LengthDelimitedCodec::new()).split();

        // 用于 call 与后台任务通信
        let (tx, mut rx) = mpsc::channel::<(Vec<Req>, oneshot::Sender<Result<Vec<Result<Res, String>>, String>>)>(1024);

        // 用于存放等待响应的回调队列 (FIFO)
        let (cb_tx, mut cb_rx) = mpsc::channel::<oneshot::Sender<Result<Vec<Result<Res, String>>, String>>>(1024);

        // --- 写任务 ---
        tokio::spawn(async move {
            while let Some((reqs, cb)) = rx.recv().await {
                match bincode2::serialize(&reqs) {
                    Ok(bytes) => {
                        if sink.send(Bytes::from(bytes)).await.is_err() {
                            let _ = cb.send(Err("Send failed: Connection closed".to_string()));
                            break;
                        }
                        // 发送成功后，将回调加入队列，交给读任务处理
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

        // --- 读任务 ---
        tokio::spawn(async move {
            while let Some(cb) = cb_rx.recv().await {
                match stream.next().await {
                    Some(Ok(frame_bytes)) => {
                        // 对应服务端的 Result<Vec<Result<Res, String>>, String>
                        let res: Result<Vec<Result<Res, String>>, String> =
                            bincode2::deserialize(&frame_bytes).unwrap_or_else(|e| {
                                Err(format!("Deserialize error: {}", e))
                            });
                        let _ = cb.send(res);
                    }
                    _ => {
                        let _ = cb.send(Err("Read failed: Connection closed".to_string()));
                        break;
                    }
                }
            }
        });

        Ok(Self { tx })
    }

    /// 发送一批请求并获得对应的一批结果
    pub async fn call(&self, requests: Vec<Req>) -> Result<Vec<Result<Res, String>>, String> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((requests, tx)).await.map_err(|_| "Client channel closed".to_string())?;
        rx.await.map_err(|_| "Response channel closed".to_string())?
    }
}