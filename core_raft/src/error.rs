use std::pin::Pin;

use bytes::BytesMut;

use crate::network::raft_rocksdb::TypeConfig;

#[derive(Debug, thiserror::Error)]
pub enum CoreRaftError {
    /*
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    */
    #[error("Error: bincode2 => {0}")]
    Bincode2(#[from] Box<bincode2::ErrorKind>),

    #[error("Error: Config already initialized")]
    ConfigAlreadyInitialized,

    //#[error("Error: openraft => {0}")]
    //OpenraftRaftError(#[from] openraft::error::RaftError<TypeConfig>),
    #[error("Error: rpc connection closed error => {0}")]
    RpcConnectionClosed(#[from] tokio::sync::mpsc::error::SendError<BytesMut>),

    #[error("Error: Rpc invalid function id => {0}")]
    RpcInvalidFuncId(u32),

    #[error("Error: Rpc package length insufficient")]
    RpcPackageLengthInsufficient,

    #[error("Error: Rpc waited canceled or connection closed => {0}")]
    RpcWaitedCanceledOrConnectionClosed(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Error: Rpc write task ended or connection closed")]
    RpcWriteTaskEndedOrConnectionClosed,

    #[error("Std io error: {0}")]
    StdIoError(#[from] std::io::Error),

    #[error("Error: Option is None")]
    StdOptionIsNone,

    #[error("Serde yaml error: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
}

//pub type BoxStdError = Box<dyn std::error::Error>;

pub type PinBoxFuture<'fut, T> = Pin<Box<dyn Future<Output = T> + Send + 'fut>>;
pub type PinBoxFutureStatic<T> = PinBoxFuture<'static, T>;

pub type CoreRaftResult<T> = Result<T, CoreRaftError>;
