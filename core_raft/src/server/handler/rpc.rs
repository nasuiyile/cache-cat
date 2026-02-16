use crate::network::node::App;
use crate::server::core::config::init_config;
use crate::server::handler::external_handler::HANDLER_TABLE;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument;

pub async fn start_server(app: App, addr: String) -> std::io::Result<()> {
    // 初始化配置（保留原有逻辑）
    // let _ = init_config("./server/config.yml");
    // let config = get_config();
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on: {}", listener.local_addr()?);
    loop {
        let app = app.clone();
        let (socket, peer_addr) = match listener.accept().await {
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
            // 使用 LengthDelimitedCodec -> 自动处理 4-byte length prefix（frame 中不含 length）
            let codec = LengthDelimitedCodec::new();
            let framed = Framed::new(socket, codec);
            // split 为读写两部分
            let (writer, mut reader) = framed.split();

            // channel 用于把要写入的 payload（不含长度头）从各处理任务发送到写任务
            let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
            let tx_for_handling = tx.clone();
            // 写任务：负责把 payload 通过 framed sink 发送出去（codec 会添加长度头）
            tokio::spawn(async move {
                let mut writer = writer;
                while let Some(payload) = rx.recv().await {
                    if let Err(e) = writer.send(payload).await {
                        eprintln!("写入 TCP（sink）失败 ({}): {}", peer_addr, e);
                        break;
                    }
                }
                // 当 rx 关闭或发生错误，writer 会被丢弃，连接结束
                tracing::info!("写任务结束: {}", peer_addr);
            });
            while let Some(frame_result) = reader.next().await {
                match frame_result {
                    Ok(mut frame_bytes) => {
                        // frame_bytes 是 BytesMut（不含 length header）。
                        // 克隆 tx 并交给处理任务（保留并发）
                        let tx = tx_for_handling.clone();
                        let app = app.clone();
                        // freeze -> Bytes，避免复制
                        let package = frame_bytes.freeze();
                        tokio::spawn(async move {
                            let start = Instant::now();
                            if let Err(_) = hand(app, tx, package).await {
                                eprintln!("处理请求失败 {}", peer_addr);
                            }
                            tracing::info!("rpc处理用时: {} 微秒", start.elapsed().as_micros());
                        }.instrument(tracing::info_span!("rpc处理", peer_addr = peer_addr.to_string())));
                    }
                    Err(e) => {
                        eprintln!("读取帧失败 ({}): {}", peer_addr, e);
                        break;
                    }
                }
            }
            tracing::info!("读任务结束: {}", peer_addr);
        });
    }
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

    let response_data = handler.call(app, package).await;

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
