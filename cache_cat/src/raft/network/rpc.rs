use crate::protocol::command::CommandFactory;
use crate::protocol::resp::Parser;
use crate::raft::network::external_handler::{HANDLER_TABLE, batch_write, write};
use crate::raft::store::snapshot::snapshot_handler::get_snapshot_file_name;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::{CacheCatApp, TypeConfig};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
use moka::future::FutureExt;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
pub struct Server {
    pub(crate) app: Arc<CacheCatApp>,
    pub addr: String,
    pub startup_tx: Sender<StdResult<(), String>>,
    pub redis_server: RedisServer,
}
impl Server {
    pub fn new(
        app: Arc<CacheCatApp>,
        addr: String,
        startup_tx: Sender<StdResult<(), String>>,
        redis_addr: String,
    ) -> Self {
        Server {
            app: app.clone(),
            addr,
            startup_tx,
            redis_server: RedisServer {
                app: app.clone(),
                redis_addr,
                cmd_factory: Arc::new(CommandFactory::init()),
            },
        }
    }
    pub async fn start_server(
        self: Self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> std::io::Result<()> {
        tokio::spawn(async move {
            Arc::new(self.redis_server)
                .start_redis_server()
                .await
                .expect("Redis : panic message");
        });

        // 初始化配置（保留原有逻辑）
        let listener = match TcpListener::bind(self.addr.clone()).await {
            Ok(l) => l,
            Err(err) => {
                let err_msg = format!("Failed to bind TCP server to {}: {}", self.addr, err);
                let _ = self.startup_tx.send(Err(err_msg));
                return Err(err);
            }
        };
        let _ = self.startup_tx.send(Ok(()));
        println!("Listening on: {}", listener.local_addr()?);
        loop {
            tokio::select! {
                // 监听连接
                res = listener.accept() => {
                    match res {
                        Ok((socket, peer_addr)) => {
                            let app = self.app.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(app, socket, peer_addr).await {
                                    error!("Error handling connection from {}: {}", peer_addr, e);
                                }
                            });
                        }
                        Err(e) => error!("Accept error: {}", e),
                    }
                }
                // 监听关闭信号
                _ = shutdown_rx.recv() => {
                    info!("Server loop stopping...");
                    break; // 跳出循环，正常结束
                }
            }
        }
        Ok(())
    }
}
async fn handle_connection(
    app: Arc<CacheCatApp>,
    mut socket: TcpStream,
    peer_addr: SocketAddr,
) -> std::io::Result<()> {
    if let Err(e) = socket.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
    }

    debug!("New connection from {}", peer_addr);

    // 读取第一个字节识别模式
    let mut protocol_byte = [0u8; 1];
    socket.read_exact(&mut protocol_byte).await?;

    if protocol_byte[0] == 0 {
        // RPC 模式
        rpc_mode(app, socket, peer_addr).await;
    } else if protocol_byte[0] == 1 {
        // Stream (Snapshot) 模式
        stream_mode(app, socket, peer_addr).await?;
    } else if protocol_byte[0] == 2 {
        pipeline_mode(app, socket, peer_addr).await;
    }

    Ok(())
}

async fn pipeline_mode(app: Arc<CacheCatApp>, socket: TcpStream, peer_addr: SocketAddr) {
    let codec = LengthDelimitedCodec::new();
    let framed = Framed::new(socket, codec);
    let (mut writer, mut reader) = framed.split();

    // 直接在这里维护队列，不再需要 mpsc
    let mut pending_futures = FuturesOrdered::new();

    loop {
        tokio::select! {
            // 1. 尝试从网络读取新的请求
            // 注意：只有当 pending_futures 还没满时才读取，起到背压作用
            frame_result = reader.next(), if pending_futures.len() < 100 => {
                match frame_result {
                    Some(Ok(frame_bytes)) => {
                        let request: Request = bincode2::deserialize(&frame_bytes).expect("Failed to deserialize");
                        // 直接推进队列，不经过 channel
                        let future = write(app.clone(), request).boxed();
                        pending_futures.push_back(future);
                    }
                    Some(Err(e)) => {
                        eprintln!("读取帧失败 ({}): {}", peer_addr, e);
                        break;
                    }
                    None => break, // 连接关闭
                }
            }

            // 2. 检查是否有执行完的结果需要写回客户端
            // FuturesOrdered 会保证即便 Future 执行快慢不一，返回顺序也和推入顺序一致
            Some(res) = pending_futures.next(), if !pending_futures.is_empty() => {
                let encoded = bincode2::serialize(&res).unwrap();
                if let Err(e) = writer.send(Bytes::from(encoded)).await {
                    eprintln!("写入 TCP 失败 ({}): {}", peer_addr, e);
                    break;
                }
            }

            // 如果两端都关闭了，退出
            else => break,
        }
    }
    debug!("Pipeline mode ended for {}", peer_addr);
}
async fn rpc_mode(app: Arc<CacheCatApp>, socket: TcpStream, peer_addr: SocketAddr) {
    let codec = LengthDelimitedCodec::new();
    let framed = Framed::new(socket, codec);

    let (writer, mut reader) = framed.split();

    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let tx_for_handling = tx.clone();

    // 写任务
    tokio::spawn(async move {
        let mut writer = writer;
        while let Some(payload) = rx.recv().await {
            if let Err(e) = writer.send(payload).await {
                eprintln!("写入 TCP 失败 ({}): {}", peer_addr, e);
                break;
            }
        }
        debug!("写任务结束: {}", peer_addr);
    });
    // 读循环（完全复用）
    while let Some(frame_result) = reader.next().await {
        match frame_result {
            Ok(frame_bytes) => {
                let tx = tx_for_handling.clone();
                let app = app.clone();
                let package = frame_bytes.freeze();

                tokio::spawn(async move {
                    if let Err(_) = hand(app, tx, package).await {
                        eprintln!("处理请求失败 {}", peer_addr);
                    }
                });
            }
            Err(e) => {
                eprintln!("读取帧失败 ({}): {}", peer_addr, e);
                break;
            }
        }
    }

    debug!("RPC读任务结束: {}", peer_addr);
}

/// hand 函数现在期望接收到的 `package` 已经是不带长度头的一帧数据（即：request_id(4) + func_id(4) + body）
/// 并通过 tx 发送回写任务一个 payload（也不包含长度头），写任务会交给 codec 自动添加长度头。
pub async fn hand(
    app: Arc<CacheCatApp>,
    tx: UnboundedSender<Bytes>,
    mut package: Bytes,
) -> Result<(), ()> {
    // 安全解析：至少需要 8 bytes (request_id + func_id)
    if package.len() < 8 {
        eprintln!("包长度不足：{}", package.len());
        return Err(());
    }

    // 使用 bytes 库的内置方法，减少手动切片和拷贝
    let request_id = package.get_u32(); // 自动前进 4 字节
    let func_id = package.get_u32(); // 自动再前进 4 字节

    // 查找 handler 并调用
    let handler = HANDLER_TABLE
        .iter()
        .find(|(id, _)| *id == func_id)
        .map(|(_, ctor)| ctor())
        .ok_or(())?;

    let response_data = handler.internal_call(app, package).await;

    // 构造要发送给客户端的 payload：request_id(4) + response_data
    let mut payload = BytesMut::with_capacity(4 + response_data.len());
    payload.put_u32(request_id);
    payload.put(response_data);

    // 发给写任务（注意：这里发送的是不含长度头的 payload，LengthDelimitedCodec 会自动在实际 socket 上写入长度头）
    if tx.send(payload.freeze()).is_err() {
        // 写任务可能已结束或连接已关闭
        return Err(());
    }
    Ok(())
}
async fn stream_mode(
    app: Arc<CacheCatApp>,
    mut socket: TcpStream,
    peer_addr: SocketAddr,
) -> std::io::Result<()> {
    let path = app.path.clone();
    let snapshot_dir = path.join("snapshot");

    // 确保目录存在
    fs::create_dir_all(&snapshot_dir).await?;
    let mut buf = [0u8; 16];
    socket.read_exact(&mut buf).await?;
    let uuid = Uuid::from_bytes(buf);
    // 临时文件名
    let temp_filename = format!("hardlink_snapshot_{}.tmp", uuid);
    let final_filename = get_snapshot_file_name();

    let temp_path = snapshot_dir.join(&temp_filename);
    let final_path = snapshot_dir.join(&final_filename);

    // 写入临时文件
    let mut file = File::create(&temp_path).await?;
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            break; // 正常关闭
        }
        file.write_all(&buf[..n]).await?;
    }

    file.flush().await?;
    // 确保文件完全持久化,可能持续很长时间
    file.sync_all().await?;

    // 关键：通过rename原子替换目标文件
    // fs::rename(&temp_path, &final_path).await?;
    tracing::info!(
        "接收到来自{}的文件 文件接收完成: {}",
        peer_addr,
        final_path.to_string_lossy()
    );
    //将生成的uuid返回给调用方
    socket.write_all(uuid.as_bytes()).await?;
    Ok(())
}

pub struct RedisServer {
    pub(crate) app: Arc<CacheCatApp>,
    redis_addr: String,
    pub cmd_factory: Arc<CommandFactory>,
}
impl RedisServer {
    async fn process_command(&self, value: Value) -> Value {
        self.cmd_factory.execute(value, self).await
    }
    /// Handle a single client connection
    async fn handle_connection(
        self: Arc<Self>,
        mut stream: TcpStream,
        peer_addr: SocketAddr,
    ) -> IoResult<()> {
        let mut buffer = vec![0u8; 8192]; // 8KB buffer
        let mut pending = Vec::new(); // Buffer for incomplete commands

        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    info!("Connection closed by client: {}", peer_addr);
                    break;
                }
                Ok(n) => {
                    // Append new data to pending buffer
                    pending.extend_from_slice(&buffer[..n]);

                    // Try to parse and process complete commands
                    let mut processed = 0;
                    while let Some((value, consumed)) = Parser::parse(&pending[processed..]) {
                        processed += consumed;

                        // Log the parsed command
                        debug!("Received command from {}: {:?}", peer_addr, value);

                        // Process the command and get response
                        let response = self.process_command(value).await;
                        let encoded = response.encode();

                        // Send response
                        if let Err(e) = stream.write_all(&encoded).await {
                            warn!("Failed to write response to {}: {}", peer_addr, e);
                            break;
                        }
                    }

                    // Remove processed data from pending buffer
                    if processed > 0 {
                        pending = pending.split_off(processed);
                    }
                }
                Err(e) => {
                    error!("Error reading from {}: {}", peer_addr, e);
                    break;
                }
            }
        }

        info!("Connection handler ended for {}", peer_addr);
        Ok(())
    }
    pub async fn start_redis_server(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.redis_addr.clone()).await?;
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("New connection accepted from {}", peer_addr);
                    // Clone the Arc<Server> for the new connection
                    let server = Arc::clone(&self);
                    // Spawn an independent task for each connection
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream, peer_addr).await {
                            error!("Error handling connection from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
