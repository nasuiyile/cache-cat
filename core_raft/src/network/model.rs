use crate::protocol::string::set::SetParams;
use crate::server::handler::model::{DelReq, LPushReq, LPushRes, SetReq, SetRes};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub enum Request {
    Set(SetReq),
    LPush(LPushReq),
    Del(DelReq),

    RedisSet(SetParams),
}
impl Request {
    pub fn set(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Request::Set(SetReq {
            key: Arc::from(key.into()),
            value: Arc::from(value.into()),
            ex_time: 0,
        })
    }
    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Set(req) => write!(f, "Set: {}", req),
            Request::LPush(req) => write!(f, "LPush: {}", req),
            Request::RedisSet(req) => write!(f, "RedisSet: {}", req),
            Request::Del(req) => write!(f, "DEL:{}", req),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicRequest {
    pub request: Request,
    pub version: u32,
}

/// A response from the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// Simple strings, used for simple responses like "OK"
    SimpleString(String),
    /// Errors
    Error(String),
    /// Integers
    Integer(i64),
    /// Bulk strings, used for binary-safe strings (can be null)
    BulkString(Option<Vec<u8>>),
    /// Arrays of other values (can be null)
    Array(Option<Vec<Value>>),
    Null,
}

impl Value {
    pub fn none() -> Self {
        Value::Null
    }
    /// Create a simple OK response
    pub fn ok() -> Self {
        Value::SimpleString("OK".to_string())
    }

    /// Create an error response
    pub fn error(msg: impl Into<String>) -> Self {
        Value::Error(msg.into())
    }

    /// Encode Value to RESP bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_to(&mut buf);
        buf
    }

    fn encode_to(&self, buf: &mut Vec<u8>) {
        match self {
            Value::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Error(e) => {
                buf.push(b'-');
                buf.extend_from_slice(e.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                buf.push(b':');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Value::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Value::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Value::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_to(buf);
                }
            }
            Value::Null => {}
        }
    }
}
