use crate::server::core::config::ONE;
use std::error::Error;
use std::path::Path;
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn send_file_once<P: AsRef<Path>>(
    addr: &str,
    group_id: u32,
    file_path: P,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
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
    let bytes_copied = io::copy(&mut file, &mut stream).await?;

    // 刷新并关闭写端，通知服务端 EOF
    stream.shutdown().await?;
    Ok(bytes_copied)
}
#[tokio::test]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // 示例用法（替换为实际地址、group_id、文件路径）
    let group_id = 1;
    let path = "E:/tmp/raft/1.png";
    match send_file_once(ONE, group_id, path).await {
        Ok(n) => println!("文件发送完成，字节数：{}", n),
        Err(e) => eprintln!("发送失败: {}", e),
    }
    Ok(())
}
