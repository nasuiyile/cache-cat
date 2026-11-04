use crate::raft::network::client::connect_cluster;
use crate::raft::store::snapshot::snapshot_handler::open_snapshot;
use crate::raft::types::raft_types::SnapshotMeta;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs, io};
use tokio_rustls::TlsConnector;
use uuid::Uuid;

/// 发送硬链接文件到其他节点的辅助结构体。
///
/// - 创建时会产生硬链接（async 构造函数 new ）
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
    /// - 否则创建硬链接并返回 Ok(Some(FileOperator))
    pub async fn new<P: AsRef<Path>>(file_path: P) -> Result<Option<Self>, io::Error> {
        let file_path = file_path.as_ref();
        let snapshot_path = file_path.join("snapshot").join("snapshot.bin");
        let uuid = Uuid::new_v4();
        let hardlink_path = Self::hard_link_path(file_path, uuid);
        // 创建硬链接。snapshot.bin 由 promote_snapshot_file 用 rename 原子替换，
        // 不存在只可能是还没生成过快照；其他错误（权限等）如实返回，不能当成"没有快照"。
        match fs::hard_link(&snapshot_path, &hardlink_path).await {
            // 只有成功创建硬链接后才交给 FileOperator 管理其生命周期。
            Ok(()) => Ok(Some(Self {
                file_path: file_path.to_path_buf(),
                uuid,
            })),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    fn hard_link_path(path: &Path, uuid: Uuid) -> PathBuf {
        path.join(format!("hardlink_snapshot_{uuid}.tmp"))
    }

    pub fn get_hard_link_buf(&self) -> PathBuf {
        Self::hard_link_path(&self.file_path, self.uuid)
    }

    //在收到快照后从节点安装的时候会调用这个方法来获得新的硬链接路径
    pub fn get_local_hard_link_buf(&self, path: &Path) -> PathBuf {
        Self::hard_link_path(&path.join("snapshot"), self.uuid)
    }

    /// 发送文件（使用硬链接路径），返回 send_file_once 的结果（成功时返回 Uuid）。
    /// 注意：这里不删除硬链接，删除由 Drop 完成。
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
    Ok(open_snapshot(path.as_ref())
        .await?
        .map(|(_, meta, _)| meta.meta))
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

        match std::fs::remove_file(&hardlink_path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => tracing::info!(
                "FileOperator: failed to remove hardlink {}: {}",
                hardlink_path.display(),
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absent_snapshot_and_removed_hardlink_are_silent() {
        let dir = tempfile::tempdir().unwrap();
        let logs = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(logs.reopen().unwrap()))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        assert!(FileOperator::new(dir.path()).await.unwrap().is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        let snapshot_dir = dir.path().join("snapshot");
        std::fs::create_dir(&snapshot_dir).unwrap();
        std::fs::write(snapshot_dir.join("snapshot.bin"), b"snapshot").unwrap();
        let operator = FileOperator::new(dir.path()).await.unwrap().unwrap();
        std::fs::remove_file(operator.get_hard_link_buf()).unwrap();
        drop(operator);

        assert!(std::fs::read(logs.path()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn drop_removes_only_the_owned_hardlink() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_dir = dir.path().join("snapshot");
        std::fs::create_dir(&snapshot_dir).unwrap();
        let snapshot_path = snapshot_dir.join("snapshot.bin");
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let first = FileOperator::new(dir.path()).await.unwrap().unwrap();
        let second = FileOperator::new(dir.path()).await.unwrap().unwrap();
        let first_path = first.get_hard_link_buf();
        let second_path = second.get_hard_link_buf();
        assert_ne!(first_path, second_path);
        assert_eq!(std::fs::read(&first_path).unwrap(), b"snapshot");

        drop(first);
        assert!(!first_path.exists());
        assert_eq!(std::fs::read(&second_path).unwrap(), b"snapshot");
        drop(second);
        assert!(!second_path.exists());
        assert_eq!(std::fs::read(&snapshot_path).unwrap(), b"snapshot");
    }

    #[tokio::test]
    async fn unexpected_creation_and_cleanup_errors_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("snapshot").join("snapshot.bin");
        std::fs::create_dir_all(&snapshot_path).unwrap();
        // A directory cannot be used as a snapshot hardlink source.
        let error = FileOperator::new(dir.path()).await.unwrap_err();
        assert_ne!(error.kind(), io::ErrorKind::NotFound);

        std::fs::remove_dir(&snapshot_path).unwrap();
        std::fs::write(&snapshot_path, b"snapshot").unwrap();
        let operator = FileOperator::new(dir.path()).await.unwrap().unwrap();
        let hardlink_path = operator.get_hard_link_buf();
        std::fs::remove_file(&hardlink_path).unwrap();
        std::fs::create_dir(&hardlink_path).unwrap();

        let logs = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(std::sync::Mutex::new(logs.reopen().unwrap()))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        drop(operator);
        assert!(
            std::fs::read_to_string(logs.path())
                .unwrap()
                .contains("FileOperator: failed to remove hardlink")
        );
    }
}
