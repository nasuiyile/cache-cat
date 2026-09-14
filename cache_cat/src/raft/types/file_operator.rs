use crate::raft::network::client::connect_cluster;
use crate::raft::types::raft_types::SnapshotMeta;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};
use tokio_rustls::TlsConnector;
use uuid::Uuid;

const CACHE_MAGIC_NUM: &[u8; 4] = b"MCDC";

const VERSION: u8 = 1;

/// 发送硬链接文件到其他节点的辅助结构体。
///
/// - 创建时会产生硬链接（async 构造函数 try_create ）
/// - drop 时会删除硬链接（同步删除）
///
/// FileOperator可以直接在内部使用或发送给客户端，但是客户端收到后要修改file_path
#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct FileOperator {
    file_path: PathBuf,
    uuid: Uuid,
}

impl FileOperator {
    /// - 如果原文件不存在，返回 Ok(None)
    /// - 否则创建硬链接并返回 Ok(Some(HardlinkSender))
    pub async fn new<P: AsRef<Path>>(file_path: P) -> Result<Option<Self>, io::Error> {
        let snapshot_path = file_path.as_ref().join("snapshot").join("snapshot.bin");
        let operator = Self {
            file_path: file_path.as_ref().to_path_buf(),
            uuid: Uuid::new_v4(),
        };
        // 构造唯一硬链接路径
        let hardlink_path = operator.get_hard_link_buf();
        // 创建硬链接。snapshot.bin 由 promote_snapshot_file 用 rename 原子替换，
        // 不存在只可能是还没生成过快照；其他错误（权限等）如实返回，不能当成"没有快照"。
        match fs::hard_link(&snapshot_path, &hardlink_path).await {
            Ok(()) => Ok(Some(operator)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub fn get_hard_link_buf(&self) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}.tmp", self.uuid);

        self.file_path.join(hardlink_filename)
    }

    //在收到快照后从节点安装的时候会调用这个方法来获得新的硬链接路径
    pub fn get_local_hard_link_buf(&self, path: &Path) -> PathBuf {
        let hardlink_filename = format!("hardlink_snapshot_{}.tmp", self.uuid);

        path.join("snapshot").join(hardlink_filename)
    }

    /// 发送文件（使用硬链接路径），返回 send_file_once 的结果（成功时返回 Uuid）。
    /// 注意：这里不删除硬链接，删除由 Drop 完成（或手动调用 close）。
    pub async fn send_file(
        &self,
        addr: &str,
        tls_connector: Option<TlsConnector>,
    ) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
        let hardlink_path = self.get_hard_link_buf();
        let uuid = send_file_once(addr, hardlink_path, self.uuid, tls_connector).await?;
        Ok(uuid)
    }
    pub async fn load_meta_data(&self) -> Result<Option<SnapshotMeta>, io::Error> {
        load_meta_from_path(self.get_hard_link_buf()).await
    }
}
pub async fn load_meta_from_path<P>(path: P) -> Result<Option<SnapshotMeta>, io::Error>
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
        return Err(io::Error::other("invalid file magic"));
    }
    let mut version = [0u8; 1];
    reader.read_exact(&mut version).await?;
    if version[0] != VERSION {
        return Err(io::Error::other("unsupported version"));
    }

    let meta_len = reader.read_u32().await? as usize;
    let mut meta_buf = vec![0u8; meta_len];
    reader.read_exact(&mut meta_buf).await?;
    let meta: SnapshotMeta = bincode2::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Some(meta))
}

//发送的时候一定要转u32
pub async fn send_file_once<P: AsRef<Path>>(
    addr: &str,
    file_path: P,
    uuid: Uuid,
    tls_connector: Option<TlsConnector>,
) -> Result<Uuid, Box<dyn Error + Send + Sync>> {
    // 连接。和 RPC 客户端走同一套 TLS 逻辑：服务端在 tls-replication 打开时
    // 会对 raft 端口上的所有连接先做 TLS 握手，裸 TCP 会直接失败
    let mut stream = connect_cluster(addr, tls_connector).await?;
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
