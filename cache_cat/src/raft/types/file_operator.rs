use crate::raft::types::raft_types::{ TypeConfig};
use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::{fs, io};
use uuid::Uuid;

const CACHE_MAGIC_NUM: &[u8; 4] = b"MCDC";

const VERSION: u8 = 1;

/// 发送硬链接文件到其他节点的辅助结构体。
/// - 创建时会产生硬链接（async 构造函数 try_create ）
/// - drop 时会删除硬链接（同步删除）
/// FileOperator可以直接在内部使用或发送给客户端，但是客户端收到后要修改file_path
#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct FileOperator {
    file_path: PathBuf,
    uuid: Uuid,
}

impl FileOperator {
    /// - 如果原文件不存在，返回 Ok(None)
    /// - 否则创建硬链接并返回 Ok(Some(HardlinkSender))
    pub async fn new<P: AsRef<Path>>(
        file_path: P,
    ) -> Result<Option<Self>, io::Error> {
        let snapshot_path = file_path
            .as_ref()
            .join("snapshot")
            .join("snapshot.bin");
        // println!("snapshot_path: {}", snapshot_path.display());
        // 1. 检查文件是否存在
        match fs::metadata(&snapshot_path).await {
            Ok(_) => {
                let operator = Self {
                    file_path: file_path.as_ref().to_path_buf(),
                    uuid: Uuid::new_v4(),
                };
                // 2. 构造唯一硬链接路径
                let hardlink_path = operator.get_hard_link_buf();
                // 3. 创建硬链接
                fs::hard_link(snapshot_path, &hardlink_path).await?;
                // 4. 返回构造完成的结构体
                Ok(Some(operator))
            }
            // 文件不存在时返回 None
            Err(_) => Ok(None),
        }
    }
    pub fn get_hard_link_buf(&self) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}.tmp", self.uuid);
        let hardlink_path = self.file_path.join(hardlink_filename);
        hardlink_path
    }

    //在收到快照后从节点安装的时候会调用这个方法来获得新的硬链接路径
    pub fn get_local_hard_link_buf(&self, path: &PathBuf) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}.tmp", self.uuid);
        let hardlink_path = path.join("snapshot").join(hardlink_filename);
        hardlink_path
    }

    /// 发送文件（使用硬链接路径），返回 send_file_once 的结果（成功时返回 Uuid）。
    /// 注意：这里不删除硬链接，删除由 Drop 完成（或手动调用 close）。
    pub async fn send_file(&self, addr: &str) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
        let hardlink_path = self.get_hard_link_buf();
        let uuid = send_file_once(addr, hardlink_path, self.uuid).await?;
        Ok(uuid)
    }
    pub async fn load_meta_data(&self) -> Result<Option<SnapshotMeta<TypeConfig>>, io::Error> {
        load_meta_from_path(self.get_hard_link_buf()).await
    }
}
pub async fn load_meta_from_path<P>(path: P) -> Result<Option<SnapshotMeta<TypeConfig>>, io::Error>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();

    let f = match File::open(path).await {
        Ok(f) => f,
        //文件不存在
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(f);
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;
    if &magic != CACHE_MAGIC_NUM {
        return Err(io::Error::new(io::ErrorKind::Other, "invalid file magic"));
    }
    let mut version = [0u8; 1];
    reader.read_exact(&mut version).await?;
    if version[0] != VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "unsupported version"));
    }

    let meta_len = reader.read_u32().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: SnapshotMeta<TypeConfig> = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(meta))
}
//发送的时候一定要转u32
pub async fn send_file_once<P: AsRef<Path>>(
    addr: &str,
    file_path: P,
    uuid: Uuid,
) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
    // 连接
    let mut stream = TcpStream::connect(addr).await?;
    // 关闭 Nagle 以降低延迟 / 确保小包快速发出（与服务端一致）
    stream.set_nodelay(true)?;

    // 第一个字节：模式标识，服务端代码中 0 是 RPC，非 0 是 stream
    stream.write_all(&[1u8]).await?;


    //发送uuid
    stream.write_all(uuid.as_bytes()).await?;

    // 打开文件并把文件内容拷贝到 stream
    let mut file = File::open(file_path).await?;
    //零拷贝，直接将文件发送到网络缓冲区
    let _bytes_copied = io::copy(&mut file, &mut stream).await?;

    // 刷新并关闭写端，通知服务端
    stream.shutdown().await?;
    //获取返回的文件名（目前没有其他用处）
    let mut buf = [0u8; 16];
    stream.read_exact(&mut buf).await?;
    let uuid = Uuid::from_bytes(buf);
    Ok(uuid)
}

// 自动删除
impl Drop for FileOperator {
    fn drop(&mut self) {
        // 在 Drop 里不能做 async，所以用同步 std::fs::remove_file。
        // 这里忽略错误（只打印），避免在 drop 时 panic。
        let hardlink_path = self.get_hard_link_buf();

        if let Err(e) = std::fs::remove_file(&hardlink_path) {
            tracing::info!(
                //没有成功删除硬链接（正常现象）
                "HardlinkSender: failed to remove hardlink {}: {}",
                hardlink_path.display(),
                e
            );
        }
    }
}
