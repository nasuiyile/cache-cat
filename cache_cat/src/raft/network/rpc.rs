use crate::raft::network::external_handler::HANDLER_TABLE;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::{App, GroupId, get_app};
use crate::raft::store::snapshot::snapshot_handler::get_snapshot_file_name;
use crate::utils;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct Server {
    pub(crate) app: App,
    pub addr: String,
    pub redis_addr: String,
}
impl Server {
    pub async fn start_server(self: Self) -> std::io::Result<()> {
        // 初始化配置（保留原有逻辑）
        // let _ = init_config("./server/config.yml");
        // let config = get_config();
        let listener = TcpListener::bind(self.addr.clone()).await?;
        println!("Listening on: {}", listener.local_addr()?);
        loop {
            let app = self.app.clone();
            let (mut socket, peer_addr) = match listener.accept().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("接受连接失败: {}", e);
                    continue;
                }
            };
            // 读循环：从 framed stream 中取出每个帧（frame 是去除 length header 后的 payload）
            tokio::spawn(async move {
                // 关闭 Nagle
                if let Err(e) = socket.set_nodelay(true) {
                    eprintln!("set_nodelay 失败: {}", e);
                }
                println!("接收到来自 {} 的新连接", peer_addr);

                let mut first = [0u8; 1];
                if let Err(e) = socket.read_exact(&mut first).await {
                    eprintln!("读取协议字节失败 {}: {}", peer_addr, e);
                    return;
                }
                //建立连接时通过一个字段来标识模式
                if first[0] == 0 {
                    run_rpc_mode(app, socket, peer_addr).await;
                } else {
                    let result = run_stream_mode(app, socket, peer_addr).await;
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("处理连接失败: {}", e);
                        }
                    }
                }
            });
        }
    }
}

async fn run_stream_mode(
    app: App,
    mut socket: TcpStream,
    peer_addr: SocketAddr,
) -> std::io::Result<()> {
    // 读取 group_id
    let mut buf = [0u8; 4];
    socket.read_exact(&mut buf).await?;
    let group_id = u32::from_be_bytes(buf);

    let path = get_app(&app, group_id as GroupId).path.clone();
    let snapshot_dir = path.join("snapshot");

    // 确保目录存在
    fs::create_dir_all(&snapshot_dir).await?;
    let mut buf = [0u8; 16];
    socket.read_exact(&mut buf).await?;
    let uuid = Uuid::from_bytes(buf);
    // 临时文件名
    let temp_filename = format!("hardlink_snapshot_{}_{}.tmp", uuid, group_id);
    let final_filename = get_snapshot_file_name(group_id as GroupId);

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

async fn run_rpc_mode(app: App, socket: TcpStream, peer_addr: SocketAddr) {
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
                    let start = Instant::now();
                    if let Err(_) = hand(app, tx, package).await {
                        eprintln!("处理请求失败 {}", peer_addr);
                    }
                    debug!("rpc处理用时: {} 微秒", start.elapsed().as_micros());
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
pub async fn hand(app: App, tx: UnboundedSender<Bytes>, mut package: Bytes) -> Result<(), ()> {
    // 安全解析：至少需要 8 bytes (request_id + func_id)
    if package.len() < 8 {
        eprintln!("包长度不足：{}", package.len());
        return Err(());
    }

    // 读取 request_id 和 func_id（网络字节序 big-endian）
    let request_id = {
        let mut b = [0u8; 4];
        b.copy_from_slice(&package[0..4]);
        u32::from_be_bytes(b)
    };
    let func_id = {
        let mut b = [0u8; 4];
        b.copy_from_slice(&package[4..8]);
        u32::from_be_bytes(b)
    };
    // 前进 8 字节，留下 body
    package.advance(8);

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
