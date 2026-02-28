use crate::server::core::config::ONE;
use std::error::Error;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;

/// 创建硬链接，调用send_file_once发送文件，然后删除硬链接
/// 只要产生了快照，那么之后就一定会存在快照文件，这个快照文件不会被删除
pub async fn send_file_via_hardlink<P: AsRef<Path>>(
    addr: &str,
    group_id: u32,
    file_path: P,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let file_path = file_path.as_ref();
    // 检查文件是否存在
    if !fs::metadata(file_path).await.is_ok() {
        return Ok(false);
    }

    // 创建唯一的硬链接文件名
    let hardlink_path = file_path.with_extension(format!("hardlink_{}", group_id));

    // 创建硬链接
    if let Err(e) = fs::hard_link(file_path, &hardlink_path).await {
        return Err(Box::new(e));
    }

    // 使用原始send_file_once发送文件
    let result = send_file_once(addr, group_id, &hardlink_path).await;

    // 删除硬链接（无论发送是否成功）
    let _ = fs::remove_file(&hardlink_path).await;

    // 处理发送结果
    match result {
        Ok(_) => Ok(true),
        Err(e) => Err(e),
    }
}
pub async fn send_file_once<P: AsRef<Path>>(
    addr: &str,
    group_id: u32,
    file_path: P,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 连接
    let mut stream = TcpStream::connect(addr).await?;
    // 关闭 Nagle 以降低延迟 / 确保小包快速发出（与服务端一致）
    stream.set_nodelay(true)?;

    // 第一个字节：模式标识，服务端代码中 0 是 RPC，非 0 是 stream
    stream.write_all(&[1u8]).await?;

    // 紧接着发送 4 字节 group_id（big-endian）
    stream.write_all(&group_id.to_be_bytes()).await?;

    // 打开文件并把文件内容拷贝到 stream
    let mut file = File::open(file_path).await?;
    //零拷贝，直接将文件发送到网络缓冲区
    let bytes_copied = io::copy(&mut file, &mut stream).await?;

    // 刷新并关闭写端，通知服务端 EOF
    stream.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn test_send_file_once() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 示例用法（替换为实际地址、group_id、文件路径）
    let group_id = 1;
    let path = "E:/tmp/raft/1.png";
    match send_file_once(ONE, group_id, path).await {
        Ok(n) => println!("文件发送完成，字节数：{:?}", n),
        Err(e) => eprintln!("发送失败: {}", e),
    }
    Ok(())
}

#[tokio::test]
async fn test_send_file_via_hardlink() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 示例用法（替换为实际地址、group_id、文件路径）
    let group_id = 1;
    let path = "E:/tmp/raft/1.png";
    match send_file_via_hardlink(ONE, group_id, path).await {
        Ok(n) => println!("文件发送完成，字节数：{:?}", n),
        Err(e) => eprintln!("发送失败: {}", e),
    }
    Ok(())
}
