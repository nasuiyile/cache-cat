//! Error handling for RockRaft
//!
//! This module provides a unified error type that hides internal complexity
//! behind a simple, user-facing interface.

use crate::error::ErrorKind::{Internal, InvalidConfig, Protocol, RPC, Retryable, Storage, Tls};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::raft_types::TypeConfig;
use mlua::prelude::LuaError;
use openraft::error::RPCError;
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::result::Result as StdResult;

/// Public error type for RockRaft operations
///
/// This is a deep module: it presents a simple interface (just `is_retryable()`)
/// while internally handling complex error categorization and conversion.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl Error {
    /// Check if this error is retryable
    ///
    /// This is the primary decision point for error handling:
    /// - `true`: Temporary issue (leader election, network glitch), retry the operation
    /// - `false`: Permanent issue (config error, internal bug), don't retry
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Retryable { .. })
    }

    /// Get the error kind
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    // Internal constructors

    /// Create a retryable error
    pub(crate) fn retryable<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind: ErrorKind::Retryable {
                reason: RetryReason::Transient,
            },
            source: Some(Box::new(source)),
        }
    }

    /// Create a retryable error with specific reason
    pub(crate) fn retryable_with_reason(reason: RetryReason) -> Self {
        Self {
            kind: ErrorKind::Retryable { reason },
            source: None,
        }
    }

    /// Create a configuration error
    pub(crate) fn config(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::InvalidConfig(msg.into()),
            source: None,
        }
    }

    /// Create an internal error
    pub(crate) fn internal(msg: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Internal(msg.into()),
            source: None,
        }
    }

    /// Create an internal error with source
    pub(crate) fn internal_with_source<E>(msg: impl Into<String>, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind: ErrorKind::Internal(msg.into()),
            source: Some(Box::new(source)),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(ref source) = self.source {
            write!(f, ": {}", source)?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_ref().map(|s| s.as_ref() as _)
    }
}

/// Error classification - tells users how to handle the error
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ErrorKind {
    /// Temporary error that may resolve on retry
    ///
    /// Examples: leader election in progress, temporary network partition,
    /// connection refused while node is starting up
    Retryable { reason: RetryReason },

    /// Configuration error - check your settings
    ///
    /// Examples: invalid endpoint format, missing required config,
    /// node ID mismatch
    InvalidConfig(String),

    /// Internal error - typically indicates a bug
    ///
    /// Examples: serialization failure, storage corruption,
    /// invariant violation
    Internal(String),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// Storage-level error
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// RPC-level error
    #[error(transparent)]
    RPC(#[from] RpcError),

    /// TLS-related error
    #[error(transparent)]
    Tls(#[from] TlsError),
}
impl From<TlsError> for Error {
    fn from(err: TlsError) -> Self {
        Self {
            kind: ErrorKind::from(err), // 利用 ErrorKind 的 #[from] 属性
            source: None,
        }
    }
}
/// Storage-related errors (Raft, RocksDB operations)
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// Raft consensus error
    #[error("raft error: {0}")]
    Raft(String),

    /// Key not found
    #[error("key not found")]
    KeyNotFound,

    /// Failed to read from storage
    #[error("read failed: {0}")]
    ReadFailed(String),

    /// Failed to write to storage
    #[error("write failed: {0}")]
    WriteFailed(String),

    /// Failed to delete from storage
    #[error("delete failed: {0}")]
    DeleteFailed(String),
}

/// Protocol-related errors (RESP parsing, command validation)
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// Lua script execution error
    #[error("ERR Error running script (call to {0}): {1}")]
    ScriptError(&'static str, String),

    /// Lua script compilation error
    #[error("ERR Error compiling script: {0}")]
    ScriptCompileError(String),

    /// Invalid RESP format
    #[error("invalid RESP format: {0}")]
    InvalidFormat(String),

    /// Database does not exist or DB index is out of range
    #[error("ERR DB index is out of range")]
    DbNotExist,

    /// Unknown command
    #[error("unknown command '{0}'")]
    UnknownCommand(String),

    /// Wrong number of arguments for a command
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongArgCount(&'static str),

    /// Invalid argument value
    #[error("ERR invalid {0}")]
    InvalidArgument(&'static str),

    /// Syntax error in command
    #[error("ERR syntax error")]
    SyntaxError,

    /// WRONGTYPE - operation against a key holding the wrong kind of value
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error(
        "READONLY This instance is not the master. Write operations are only allowed on the master node."
    )]
    ReadOnly,

    /// Value is not a valid integer
    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    /// Numeric overflow on increment/decrement
    #[error("ERR increment or decrement would overflow")]
    Overflow,

    /// Custom error with full Redis error message
    #[error("{0}")]
    Custom(&'static str),

    #[error("ERR Client sent AUTH, but no password is set")]
    NotAuthenticated,
}

/// TLS-related errors
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum TlsError {
    /// Failed to load TLS certificate
    #[error("failed to load certificate: {0}")]
    CertificateLoad(String),

    /// Failed to load TLS private key
    #[error("failed to load private key: {0}")]
    PrivateKeyLoad(String),

    /// Failed to load CA certificate
    #[error("failed to load CA certificate: {0}")]
    CaCertificateLoad(String),

    /// Invalid TLS configuration
    #[error("invalid TLS configuration: {0}")]
    InvalidConfig(String),

    /// TLS handshake error
    #[error("TLS handshake failed: {0}")]
    Handshake(String),

    /// Generic TLS error
    #[error("TLS error: {0}")]
    General(String),
}

/// RPC-related errors
#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum RpcError {
    /// RPC call timed out
    #[error("RPC timeout")]
    Timeout,

    /// Node is unreachable
    #[error("node unreachable: {0}")]
    Unreachable(String),

    /// Network error
    #[error("network error: {0}")]
    Network(String),

    /// Remote node returned an error
    #[error("remote error: {0}")]
    Remote(String),
}

impl From<LuaError> for ProtocolError {
    fn from(err: LuaError) -> Self {
        match err {
            LuaError::SyntaxError { message, .. } => ProtocolError::ScriptCompileError(message),
            LuaError::RuntimeError(message) => ProtocolError::ScriptError("f_script", message),
            LuaError::MemoryError(message) => ProtocolError::ScriptError("unknown", message),
            // ... 其他 Lua 错误类型
            _ => ProtocolError::ScriptError("unknown", err.to_string().replace('\n', " ")),
        }
    }
}

// 这样 Error 的转换链就自动工作了
impl From<LuaError> for Error {
    fn from(err: LuaError) -> Self {
        // LuaError -> ProtocolError -> ErrorKind -> Error
        ProtocolError::from(err).into()
    }
}

impl From<ProtocolError> for Error {
    fn from(err: ProtocolError) -> Self {
        Self {
            kind: ErrorKind::from(err), // 利用 #[from]
            source: None,
        }
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Self {
            kind: ErrorKind::from(err), // 利用 #[from]
            source: None,
        }
    }
}

impl From<RpcError> for Error {
    fn from(err: RpcError) -> Self {
        Self {
            kind: ErrorKind::from(err), // 利用 #[from]
            source: None,
        }
    }
}

// RPCError<TypeConfig> 自动转换为 Error
impl From<RPCError<TypeConfig>> for Error {
    fn from(err: RPCError<TypeConfig>) -> Self {
        let rpc_err = match err {
            RPCError::Timeout(_) => RpcError::Timeout,
            RPCError::Unreachable(e) => RpcError::Unreachable(e.to_string()),
            RPCError::Network(e) => RpcError::Network(e.to_string()),
            RPCError::RemoteError(e) => RpcError::Remote(e.to_string()),
        };
        rpc_err.into()
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Retryable { reason } => write!(f, "retryable error: {}", reason),
            InvalidConfig(msg) => write!(f, "configuration error: {}", msg),
            Internal(msg) => write!(f, "internal error: {}", msg),
            Protocol(err) => write!(f, "protocol error: {}", err),
            Storage(err) => write!(f, "storage error: {}", err),
            RPC(err) => write!(f, "RPC error: {}", err),
            Tls(err) => write!(f, "TLS error: {}", err),
        }
    }
}

/// Specific reasons for retryable errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryReason {
    /// No leader currently elected
    NoLeader,
    /// Leader is changing
    LeaderTransition,
    /// Temporary network issue
    Transient,
    /// Target node is still starting up
    NodeStarting,
}

impl Display for RetryReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RetryReason::NoLeader => write!(f, "no leader available"),
            RetryReason::LeaderTransition => write!(f, "leader transition in progress"),
            RetryReason::Transient => write!(f, "temporary failure"),
            RetryReason::NodeStarting => write!(f, "target node is starting"),
        }
    }
}

/// Result type alias for RockRaft operations
pub type Result<T> = StdResult<T, Error>;

// =============================================================================
// Internal error types - not exported, used for module-internal conversions
// =============================================================================

/// Internal API errors from Raft operations
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ApiError {
    #[error("cannot forward request: {0}")]
    CannotForward(String),

    #[error("forward to leader")]
    ForwardToLeader { leader_id: Option<u64> },
}

impl ApiError {
    /// Check if this API error is retryable
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(
            self,
            ApiError::CannotForward(_) | ApiError::ForwardToLeader { .. }
        )
    }
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Self {
        if e.is_retryable() {
            let reason = match &e {
                ApiError::ForwardToLeader { .. } => RetryReason::LeaderTransition,
                _ => RetryReason::Transient,
            };
            Self {
                kind: ErrorKind::Retryable { reason },
                source: None,
            }
        } else {
            Self::internal(e.to_string())
        }
    }
}

// =============================================================================
// Conversions from external error types
// =============================================================================
impl From<CacheCatError> for Value {
    fn from(err: CacheCatError) -> Self {
        match &err.kind {
            Protocol(e) => Value::error(e.to_string()),
            // Protocol errors already contain the correct Redis error prefix
            Storage(e) => Value::error(format!("ERR {}", e)),
            Internal(e) => Value::error(format!("ERR {}", e)),
            InvalidConfig(e) => Value::error(format!("ERR {}", e)),
            Retryable { reason: e } => Value::error(format!("ERR {}", e)),
            RPC(e) => Value::error(format!("ERR {}", e)),
            Tls(e) => Value::error(format!("ERR {}", e)),
        }
    }
}

impl From<ProtocolError> for Value {
    fn from(err: ProtocolError) -> Self {
        Value::error(err.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        // IO errors are generally retryable (network issues, etc.)
        Self::retryable(e)
    }
}

impl From<bincode2::Error> for Error {
    fn from(e: bincode2::Error) -> Self {
        // Serialization errors are typically internal bugs
        Self::internal_with_source("serialization failed", e)
    }
}

// =============================================================================
// Legacy type aliases for backward compatibility during migration
// =============================================================================

/// Deprecated: Use `Error` instead
pub type CacheCatError = Error;

/// Deprecated: Use `Result<T>` instead
pub type CacheCatResult<T> = Result<T>;