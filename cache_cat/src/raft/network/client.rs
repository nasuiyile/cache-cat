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
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// --- 槽位管理器配置 ---
const MAX_PENDING: usize = 65536; // 必须是 2 的幂
const INDEX_MASK: u32 = (MAX_PENDING - 1) as u32;
const TCP_CONNECT_NUM: usize = 4;

/// 预分配的响应槽位
struct Slot {
    /// 存储响应数据
    data: Mutex<Option<Bytes>>,
    /// 存储错误信息（例如连接断开）
    error: Mutex<Option<String>>,
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
            error: Mutex::new(None),
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

    /// 连接断开时，唤醒所有正在等待的请求
    fn fail_all_pending(&self, reason: &str) {
        let reason = reason.to_string();
        for slot in &self.slots {
            if slot.occupied.swap(false, Ordering::AcqRel) {
                {
                    let mut d = slot.data.lock();
                    *d = None;
                }
                {
                    let mut e = slot.error.lock();
                    *e = Some(reason.clone());
                }
                slot.waker.wake();
            }
        }
    }
}

// --- RPC 核心实现 ---
#[derive(Default)]
pub struct RpcMultiClient {
    clients: Vec<Arc<RwLock<RpcClient>>>,
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
            clients.push(Arc::new(RwLock::new(client)));
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
            clients.push(Arc::new(RwLock::new(client)));
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
        let mut payload = BytesMut::with_capacity(128);
        bincode2::serialize_into((&mut payload).writer(), &req)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let idx = self.next_client.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();

        // 先拿一个当前客户端的快照，避免长时间持有锁
        let client_snapshot = {
            let guard = self.clients[idx].read().await;
            guard.clone()
        };

        match client_snapshot
            .call_serialized(func_id, payload.clone())
            .await
        {
            Ok(response_bytes) => Self::decode_response(response_bytes),
            Err(RPCError::Network(_)) => {
                // 网络错误：重连一次并重试
                let fresh_client = RpcClient::connect(&self.addr)
                    .await
                    .map_err(|e| RPCError::Network(NetworkError::from_string(&e.to_string())))?;
                {
                    let mut guard = self.clients[idx].write().await;
                    *guard = fresh_client.clone();
                }

                let response_bytes = fresh_client.call_serialized(func_id, payload).await?;
                Self::decode_response(response_bytes)
            }
            Err(e) => Err(e),
        }
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
        match timeout(duration, self.call(func_id, req)).await {
            Ok(result) => result,
            Err(_) => Err(RPCError::Timeout(err)),
        }
    }

    fn decode_response<Res>(response_bytes: Bytes) -> Result<Res, RPCError<TypeConfig>>
    where
        Res: DeserializeOwned,
    {
        let remote_result: Result<Res, String> = bincode2::deserialize(&response_bytes)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        remote_result.map_err(|e| RPCError::Unreachable(Unreachable::from_string(&e)))
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
        let table_writer = slot_table.clone();

        let (tx_writer, mut rx_writer) = mpsc::channel::<BytesMut>(2048);

        // 写任务：任何写失败都说明连接不可用了
        tokio::spawn(async move {
            while let Some(req) = rx_writer.recv().await {
                if sink.send(Bytes::from(req)).await.is_err() {
                    break;
                }
            }
            table_writer.fail_all_pending("connection closed while writing");
        });

        // 读任务：连接断开时唤醒所有等待中的请求
        tokio::spawn(async move {
            while let Some(frame_res) = stream.next().await {
                let mut frame = match frame_res {
                    Ok(frame) => frame,
                    Err(_) => break,
                };

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
                        let mut err = slot.error.lock();
                        *err = None;
                    }
                    {
                        let mut guard = slot.data.lock();
                        *guard = Some(body);
                    }
                    slot.waker.wake();
                }
            }

            table_reader.fail_all_pending("connection closed while reading");
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
        let mut payload = BytesMut::with_capacity(128);
        bincode2::serialize_into((&mut payload).writer(), &req)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let response_bytes = self.call_serialized(func_id, payload).await?;
        let remote_result: Result<Res, String> = bincode2::deserialize(&response_bytes)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        remote_result.map_err(|e| RPCError::Unreachable(Unreachable::from_string(&e)))
    }

    async fn call_serialized(
        &self,
        func_id: u32,
        req_buf: BytesMut,
    ) -> Result<Bytes, RPCError<TypeConfig>> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let idx = (request_id & INDEX_MASK) as usize;
        let slot = &self.slot_table.slots[idx];

        // 抢占槽位
        if slot.occupied.swap(true, Ordering::Acquire) {
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
        {
            let mut err = slot.error.lock();
            *err = None;
        }

        // 序列化帧：request_id + func_id + body
        let mut buf = BytesMut::with_capacity(8 + req_buf.len());
        buf.put_u32(request_id);
        buf.put_u32(func_id);
        buf.extend_from_slice(&req_buf);

        // 发送
        if let Err(e) = self.tx_writer.send(buf).await {
            slot.occupied.store(false, Ordering::Release);
            return Err(RPCError::Network(NetworkError::new(&e)));
        }

        // 等待响应
        let waiter = ResponseFuture { slot };
        let response_bytes = waiter.await?;
        Ok(response_bytes)
    }
}

/// 自定义 Future 避免使用 oneshot 的内存分配
struct ResponseFuture<'a> {
    slot: &'a Slot,
}

impl<'a> Drop for ResponseFuture<'a> {
    fn drop(&mut self) {
        // 无论正常完成、超时还是取消，槽位都释放
        self.slot.occupied.store(false, Ordering::Release);
    }
}

impl<'a> Future for ResponseFuture<'a> {
    type Output = Result<Bytes, RPCError<TypeConfig>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut guard = self.slot.data.lock();
            if let Some(data) = guard.take() {
                return Poll::Ready(Ok(data));
            }
        }

        {
            let mut err_guard = self.slot.error.lock();
            if let Some(err) = err_guard.take() {
                return Poll::Ready(Err(RPCError::Network(
                    NetworkError::<TypeConfig>::from_string(&err),
                )));
            }
        }

        self.slot.waker.register(cx.waker());

        {
            let mut guard = self.slot.data.lock();
            if let Some(data) = guard.take() {
                return Poll::Ready(Ok(data));
            }
        }

        {
            let mut err_guard = self.slot.error.lock();
            if let Some(err) = err_guard.take() {
                return Poll::Ready(Err(RPCError::Network(
                    NetworkError::<TypeConfig>::from_string(&err),
                )));
            }
        }

        Poll::Pending
    }
}
