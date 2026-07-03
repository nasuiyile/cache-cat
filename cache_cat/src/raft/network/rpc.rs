use crate::error::{CacheCatError, Error};
use crate::node::parsed_config::ParsedConfig;
use crate::raft::network::connection::Connection;
use crate::raft::network::external_handler::{HANDLER_TABLE, write};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::store::snapshot::snapshot_handler::get_snapshot_file_name;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::CacheCatApp;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::FutureExt;
use futures::stream::FuturesOrdered;
use futures::{SinkExt, StreamExt};
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
use tokio_rustls::TlsAcceptor;
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
        config: &ParsedConfig,
    ) -> Result<Self, CacheCatError> {
        let redis_server = match RedisServer::new(app.clone(), redis_addr, config) {
            Ok(rs) => rs,
            Err(e) => {
                let _ = startup_tx.send(Err(format!("Failed to create RedisServer: {}", e)));
                return Err(e);
            }
        };
        Ok(Server {
            app,
            addr,
            startup_tx,
            redis_server,
        })
    }

    pub async fn start_server(
        self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> std::io::Result<()> {
        tokio::spawn(async move {
            Arc::new(self.redis_server)
                .start_redis_server()
                .await
                .expect("Redis : panic message");
        });

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
                res = listener.accept() => {
                    match res {
                        Ok((socket, peer_addr)) => {
                            let app = self.app.clone();
                            let tls_acceptor = self.app.tls_context.acceptor_for_cluster();  // 获取tls_acceptor
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(app, socket, peer_addr, tls_acceptor).await {
                                    error!("Error handling connection from {}: {}", peer_addr, e);
                                }
                            });
                        }
                        Err(e) => error!("Accept error: {}", e),
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Server loop stopping...");
                    break;
                }
            }
        }
        Ok(())
    }
}

async fn handle_connection(
    app: Arc<CacheCatApp>,
    socket: TcpStream,
    peer_addr: SocketAddr,
    tls_acceptor: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    if let Err(e) = socket.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
    }
    debug!("New connection from {}", peer_addr);

    // 根据app.config.tls_replication决定是否使用TLS
    let mut connection = if app.config.tls_replication {
        // 需要TLS连接
        match tls_acceptor {
            Some(acceptor) => {
                // TLS模式：进行TLS握手
                match acceptor.accept(socket).await {
                    Ok(tls_stream) => {
                        debug!("TLS handshake successful for {}", peer_addr);
                        Connection::from(tls_stream)
                    }
                    Err(e) => {
                        error!("TLS handshake failed for {}: {}", peer_addr, e);
                        return Err(std::io::Error::other(e));
                    }
                }
            }
            None => {
                // 配置要求TLS但tls_acceptor为空，抛出错误
                error!(
                    "TLS replication is enabled but TLS acceptor is not configured for {}",
                    peer_addr
                );
                return Err(std::io::Error::other(
                    "TLS replication is enabled but TLS acceptor is not configured",
                ));
            }
        }
    } else {
        // 不需要TLS，直接使用普通TCP连接
        Connection::from(socket)
    };

    // 读取第一个字节识别模式
    let mut protocol_byte = [0u8; 1];
    connection.read_exact(&mut protocol_byte).await?;

    if protocol_byte[0] == 0 {
        // RPC 模式
        rpc_mode(app, connection, peer_addr).await;
    } else if protocol_byte[0] == 1 {
        // Stream (Snapshot) 模式
        stream_mode(app, connection, peer_addr).await?;
    } else if protocol_byte[0] == 2 {
        // Pipeline 模式
        pipeline_mode(app, connection, peer_addr).await;
    }

    Ok(())
}

// 修改pipeline_mode以接受Connection而不是TcpStream
async fn pipeline_mode(app: Arc<CacheCatApp>, connection: Connection, peer_addr: SocketAddr) {
    let codec = LengthDelimitedCodec::new();
    let framed = Framed::new(connection, codec);
    let (mut writer, mut reader) = framed.split();

    let mut pending_futures = FuturesOrdered::new();
    loop {
        tokio::select! {
            frame_result = reader.next(), if pending_futures.len() < 100 => {
                match frame_result {
                    Some(Ok(frame_bytes)) => {
                        let request: Request = bincode2::deserialize(&frame_bytes).expect("Failed to deserialize");
                        let future = write(app.clone(), request).boxed();
                        pending_futures.push_back(future);
                    }
                    Some(Err(e)) => {
                        error!("读取帧失败 ({}): {}", peer_addr, e);
                        break;
                    }
                    None => break,
                }
            }

            Some(res) = pending_futures.next(), if !pending_futures.is_empty() => {
                let encoded = bincode2::serialize(&res).unwrap();
                if let Err(e) = writer.send(Bytes::from(encoded)).await {
                    error!("写入 TCP 失败 ({}): {}", peer_addr, e);
                    break;
                }
            }

            else => break,
        }
    }
    debug!("Pipeline mode ended for {}", peer_addr);
}

// 修改rpc_mode以接受Connection而不是TcpStream
async fn rpc_mode(app: Arc<CacheCatApp>, connection: Connection, peer_addr: SocketAddr) {
    let codec = LengthDelimitedCodec::new();
    let framed = Framed::new(connection, codec);

    let (writer, mut reader) = framed.split();

    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let tx_for_handling = tx.clone();

    // 写任务
    tokio::spawn(async move {
        let mut writer = writer;
        while let Some(payload) = rx.recv().await {
            if let Err(e) = writer.send(payload).await {
                error!("写入 TCP 失败 ({}): {}", peer_addr, e);
                break;
            }
        }
        debug!("写任务结束: {}", peer_addr);
    });

    // 读循环
    while let Some(frame_result) = reader.next().await {
        match frame_result {
            Ok(frame_bytes) => {
                let tx = tx_for_handling.clone();
                let app = app.clone();
                let package = frame_bytes.freeze();

                tokio::spawn(async move {
                    if hand(app, tx, package).await.is_err() {
                        error!("处理请求失败 {}", peer_addr);
                    }
                });
            }
            Err(e) => {
                error!("读取帧失败 ({}): {}", peer_addr, e);
                break;
            }
        }
    }

    debug!("RPC读任务结束: {}", peer_addr);
}

// hand函数保持不变
pub async fn hand(
    app: Arc<CacheCatApp>,
    tx: UnboundedSender<Bytes>,
    mut package: Bytes,
) -> Result<(), CacheCatError> {
    if package.len() < 8 {
        error!("Package length insufficient：{}", package.len());
        return Err(Error::internal("Insufficient package length".to_string()));
    }

    let request_id = package.get_u32();
    let func_id = package.get_u32();

    let handler = HANDLER_TABLE
        .iter()
        .find(|(id, _)| *id == func_id)
        .map(|(_, ctor)| ctor())
        .ok_or(())
        .map_err(|_| Error::internal("Handler not found".to_string()))?;

    let response_data = handler.internal_call(app, package).await?;

    let mut payload = BytesMut::with_capacity(4 + response_data.len());
    payload.put_u32(request_id);
    payload.put(response_data);

    if tx.send(payload.freeze()).is_err() {
        return Err(Error::internal("Write task has ended".to_string()));
    }
    Ok(())
}

// 修改stream_mode以接受Connection而不是TcpStream
async fn stream_mode(
    app: Arc<CacheCatApp>,
    mut connection: Connection,
    peer_addr: SocketAddr,
) -> std::io::Result<()> {
    let path = app.path.clone();
    let snapshot_dir = path.join("snapshot");

    fs::create_dir_all(&snapshot_dir).await?;
    let mut buf = [0u8; 16];
    connection.read_exact(&mut buf).await?;
    let uuid = Uuid::from_bytes(buf);

    let temp_filename = format!("hardlink_snapshot_{}.tmp", uuid);
    let final_filename = get_snapshot_file_name();

    let temp_path = snapshot_dir.join(&temp_filename);
    let final_path = snapshot_dir.join(&final_filename);

    let mut file = File::create(&temp_path).await?;
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = connection.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n]).await?;
    }

    file.flush().await?;
    file.sync_all().await?;
    info!(
        "接收到来自{}的文件 文件接收完成: {}",
        peer_addr,
        final_path.to_string_lossy()
    );

    connection.write_all(uuid.as_bytes()).await?;
    Ok(())
}
