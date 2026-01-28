use crate::network::raft_rocksdb::CacheCatApp;
use crate::server::core::config::{get_config, init_config};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use crate::server::handler::external_handler::HANDLER_TABLE;

pub async fn start_server(app: Arc<CacheCatApp>) -> std::io::Result<()> {
    let _ = init_config("./server/config.yml");
    let config = get_config();
    let addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(app.addr.clone()).await?;
    println!("Listening on: {}", listener.local_addr()?);
    loop {
        let Ok((socket, addr)) = listener.accept().await else {
            eprintln!("接受连接失败");
            continue;
        };
        socket.set_nodelay(true).ok(); // 关闭 Nagle
        println!("接收到来自 {} 的新连接", addr);

        let (reader, writer) = socket.into_split();
        // 创建一个 channel 用于写数据
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();

        // 写任务：专门负责往 tcp 写数据
        tokio::spawn(async move {
            let mut writer = writer;
            while let Some(data) = rx.recv().await {
                if let Err(e) = writer.write_all(&data).await {
                    eprintln!("写入 TCP 失败 ({}): {}", addr, e);
                    break;
                }
            }
        });
        let app = app.clone();

        // 读任务：处理客户端请求
        tokio::spawn(async move {
            let mut reader = reader;
            let mut buffer = BytesMut::with_capacity(1024);
            loop {
                if buffer.len() < 4 {
                    if let Err(e) = reader.read_buf(&mut buffer).await {
                        //对方异常关闭也会导致这个错误
                        eprintln!("读取长度头失败 ({}): {}", addr, e);
                        break;
                    }
                    continue;
                }
                let data_length = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                buffer.advance(4);
                while buffer.len() < data_length as usize {
                    if let Err(e) = reader.read_buf(&mut buffer).await {
                        eprintln!("读取数据体失败 ({}): {}", addr, e);
                        break;
                    }
                }
                let data_packet = buffer.split_to(data_length as usize);
                let tx = tx.clone();
                // 处理请求任务
                let app = app.clone();
                tokio::spawn(async move {
                    match hand(app, tx, data_packet.freeze()).await {
                        Ok(_) => {}
                        Err(_) => {
                            eprintln!("处理请求失败 {}", addr);
                        }
                    }
                });
            }
        });
    }
}
pub async fn hand(
    app: Arc<CacheCatApp>,
    tx: UnboundedSender<Bytes>,
    mut package: Bytes,
) -> Result<(), ()> {
    // 读取 request_id(4) + func_id(4)
    let request_id = u32::from_be_bytes(package[0..4].try_into().unwrap());
    let func_id = u32::from_be_bytes(package[4..8].try_into().unwrap());
    package.advance(8);
    // 选择对应的方法并调用
    let handler = HANDLER_TABLE
        .iter()
        .find(|(id, _)| *id == func_id)
        .map(|(_, ctor)| ctor())
        .ok_or(())?;
    let response_data = handler.call(app, package).await;
    let mut response_length = response_data.len() as u32;
    // 协议中 response body 前还有 4 bytes 的 request_id
    response_length = response_length + 4;
    // BytesMut 避免重复分配内存
    let mut response_header = BytesMut::with_capacity(8 + response_data.len());
    response_header.put_u32(response_length);
    response_header.put_u32(request_id);
    response_header.put(response_data);
    let result = tx.send(response_header.freeze());
    if result.is_err() {
        return Err(());
    }
    Ok(())
}