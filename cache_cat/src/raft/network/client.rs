use crate::raft::types::raft_types::TypeConfig;
use bincode2;
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_utils::CachePadded;
use futures::task::AtomicWaker;
use futures::{SinkExt, StreamExt};
use openraft::error::Timeout;
use openraft::error::{NetworkError, RPCError, Unreachable};
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// --- 槽位管理器配置 ---
const MAX_PENDING: usize = 65536; // 必须是 2 的幂
const INDEX_MASK: u32 = (MAX_PENDING - 1) as u32;
const TCP_CONNECT_NUM: usize = 2;

/// 预分配的响应槽位
struct Slot {
    /// 存储响应数据
    data: Mutex<Option<Bytes>>,
    /// 用于唤醒正在等待的 call 任务
    waker: AtomicWaker,
    /// 标识槽位是否已被占用
    occupied: AtomicBool,
    /// 用于校验 RequestID，防止 ID 环绕导致读到旧数据
    generation: AtomicU32,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            data: Mutex::new(None),
            waker: AtomicWaker::new(),
            occupied: AtomicBool::new(false),
            generation: AtomicU32::new(0),
        }
    }
}

/// 槽位表，使用 CachePadded 防止多核竞争下的伪共享
struct SlotTable {
    slots: Vec<CachePadded<Slot>>,
}

impl SlotTable {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(MAX_PENDING);
        for _ in 0..MAX_PENDING {
            slots.push(CachePadded::new(Slot::default()));
        }
        Self { slots }
    }
}

// --- RPC 核心实现 ---
#[derive(Default)]
pub struct RpcMultiClient {
    clients: Vec<RpcClient>,
    next_client: AtomicU32,
    pub addr: String,
}
impl Clone for RpcMultiClient {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            clients: self.clients.clone(),
            next_client: AtomicU32::new(0),
        }
    }
}

impl RpcMultiClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut clients = Vec::new();
        for _ in 0..TCP_CONNECT_NUM {
            let client = RpcClient::connect(addr).await?;
            clients.push(client);
        }
        Ok(Self {
            addr: addr.to_string(),
            clients,
            next_client: AtomicU32::new(0),
        })
    }
    pub async fn connect_with_num(
        addr: &str,
        connect_num: usize,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut clients = Vec::new();
        for _ in 0..connect_num {
            let client = RpcClient::connect(addr).await?;
            clients.push(client);
        }
        Ok(Self {
            addr: addr.to_string(),
            clients,
            next_client: AtomicU32::new(0),
        })
    }

    pub async fn call<Req, Res>(&self, func_id: u32, req: Req) -> Result<Res, RPCError<TypeConfig>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let idx = self.next_client.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        self.clients[idx].call(func_id, req).await
    }
    /// 带超时的调用版本
    pub async fn call_with_timeout<Req, Res>(
        &self,
        func_id: u32,
        req: Req,
        duration: Duration,
        err: Timeout<TypeConfig>,
    ) -> Result<Res, RPCError<TypeConfig>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        // 使用 tokio::time::timeout 包装核心逻辑
        match timeout(duration, self.call(func_id, req)).await {
            Ok(result) => result,
            Err(_) => Err(RPCError::Timeout(err)),
        }
    }
}

#[derive(Clone)]
pub struct RpcClient {
    tx_writer: mpsc::Sender<BytesMut>,
    slot_table: Arc<SlotTable>,
    next_request_id: Arc<AtomicU32>,
}
impl RpcClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?; // RPC 必须关闭 Nagle 算法以降低延迟
        stream.write_all(&[0u8]).await?;
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let (mut sink, mut stream) = framed.split();

        let slot_table = Arc::new(SlotTable::new());
        let table_reader = slot_table.clone();
        let (tx_writer, mut rx_writer) = mpsc::channel::<BytesMut>(2048);

        // 写任务
        tokio::spawn(async move {
            // 先等第一个消息
            while let Some(req) = rx_writer.recv().await {
                let _ = sink.feed(Bytes::from(req)).await;
                // 贪婪地榨干当前 channel 里的积压消息
                while let Ok(req) = rx_writer.try_recv() {
                    let _ = sink.feed(Bytes::from(req)).await;
                }
                // 批量 syscall
                if sink.flush().await.is_err() {
                    break;
                }
            }
        });

        // 读任务
        tokio::spawn(async move {
            while let Some(frame_res) = stream.next().await {
                if let Ok(mut frame) = frame_res {
                    if frame.len() < 4 {
                        continue;
                    }

                    let request_id = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
                    let body = frame.split_off(4).freeze();

                    let idx = (request_id & INDEX_MASK) as usize;
                    let slot = &table_reader.slots[idx];

                    // 校验 generation 是否匹配，防止串号
                    if slot.generation.load(Ordering::Acquire) == request_id {
                        {
                            let mut guard = slot.data.lock();
                            *guard = Some(body);
                        }
                        slot.waker.wake();
                    }
                }
            }
        });

        Ok(Self {
            tx_writer,
            slot_table,
            next_request_id: Arc::new(AtomicU32::new(1)),
        })
    }

    pub async fn call<Req, Res>(&self, func_id: u32, req: Req) -> Result<Res, RPCError<TypeConfig>>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let idx = (request_id & INDEX_MASK) as usize;
        let slot = &self.slot_table.slots[idx];

        // 抢占槽位
        if slot.occupied.swap(true, Ordering::Acquire) {
            // 如果已经被占用，说明并发量超过了 MAX_PENDING 或发生了死锁
            return Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
                "too many requests",
            )));
        }

        // 初始化槽位状态
        slot.generation.store(request_id, Ordering::Release);
        {
            let mut guard = slot.data.lock();
            *guard = None;
        }

        // 序列化并发送
        let mut buf = BytesMut::with_capacity(128);
        buf.put_u32(request_id);
        buf.put_u32(func_id);
        bincode2::serialize_into((&mut buf).writer(), &req)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        if let Err(e) = self.tx_writer.send(buf).await {
            slot.occupied.store(false, Ordering::Release);
            return Err(RPCError::Network(NetworkError::new(&e)));
        }

        // 等待响应 (ResponseFuture)
        let waiter = ResponseFuture { slot };
        let response_bytes = waiter.await?;
        let remote_result: Result<Res, String> = bincode2::deserialize(&response_bytes)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        remote_result.map_err(|e| RPCError::Unreachable(Unreachable::from_string(&e)))
    }
}

/// 自定义 Future 避免使用 oneshot 的内存分配
struct ResponseFuture<'a> {
    slot: &'a Slot,
}

impl<'a> Drop for ResponseFuture<'a> {
    fn drop(&mut self) {
        // 确保 Future 消失时（无论是正常完成还是超时/取消），槽位都会释放
        self.slot.occupied.store(false, Ordering::Release);
    }
}

impl<'a> Future for ResponseFuture<'a> {
    type Output = Result<Bytes, RPCError<TypeConfig>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.slot.data.lock();

        if let Some(data) = guard.take() {
            // 注意：这里不再手动 store(false)，交给 Drop 处理或在此处处理皆可
            // 为了逻辑清晰，我们在 Ready 时处理，并在 Drop 中做检查
            return Poll::Ready(Ok(data));
        }

        self.slot.waker.register(cx.waker());

        if let Some(data) = guard.take() {
            return Poll::Ready(Ok(data));
        }

        Poll::Pending
    }
}
