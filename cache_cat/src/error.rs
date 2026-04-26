//! Error handling for RockRaft
//!
//! This module provides a unified error type that hides internal complexity
//! behind a simple, user-facing interface.

use std::error::Error as StdError;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io;
use std::result::Result as StdResult;
use crate::error::ErrorKind::{Internal, InvalidConfig, Protocol, Retryable, Storage};
use crate::raft::types::core::response_value::Value;

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
    /// Invalid RESP format
    #[error("invalid RESP format: {0}")]
    InvalidFormat(String),

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

    /// Value is not a valid integer
    #[error("ERR value is not an integer or out of range")]
    NotAnInteger,

    /// Numeric overflow on increment/decrement
    #[error("ERR increment or decrement would overflow")]
    Overflow,

    /// Custom error with full Redis error message
    #[error("{0}")]
    Custom(&'static str),
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

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Retryable { reason } => write!(f, "retryable error: {}", reason),
            ErrorKind::InvalidConfig(msg) => write!(f, "configuration error: {}", msg),
            ErrorKind::Internal(msg) => write!(f, "internal error: {}", msg),
            ErrorKind::Protocol(err) => write!(f, "protocol error: {}", err),
            ErrorKind::Storage(err) => write!(f, "storage error: {}", err),
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
        }
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
