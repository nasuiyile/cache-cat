use crate::network::model::{Request, Value};
use crate::network::node::get_group_by_key;
use crate::protocol::command::Command;
use crate::server::handler::rpc::Server;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::{Hash, Hasher};

/// Expiration time options for SET command
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expiration {
    /// EX seconds - Set the specified expire time, in seconds
    Ex(u64),
    /// PX milliseconds - Set the specified expire time, in milliseconds
    Px(u64),
    /// EXAT timestamp-seconds - Set the specified Unix time at which the key will expire, in seconds
    ExAt(u64),
    /// PXAT timestamp-milliseconds - Set the specified Unix time at which the key will expire, in milliseconds
    PxAt(u64),
    /// KEEPTTL - Retain the time to live associated with the key
    KeepTTL,
}

/// Set mode options (NX/XX)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetMode {
    /// NX - Only set the key if it does not already exist
    Nx,
    /// XX - Only set the key if it already exists
    Xx,
}

/// Parameters for SET command
///
/// Standard Redis SET command format:
/// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetParams {
    /// The key to set
    pub key: Vec<u8>,
    /// The value to set
    pub value: Vec<u8>,
    /// NX or XX mode (optional)
    pub mode: Option<SetMode>,
    /// Whether to return the previous value (GET option)
    pub get: bool,
    /// Expiration time options (optional)
    pub expiration: Option<Expiration>,
}
impl Hash for SetParams {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}
impl Display for SetParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SET {} {}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

impl SetParams {
    /// Create a new SetParams with minimal required fields
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            mode: None,
            get: false,
            expiration: None,
        }
    }

    /// Parse SET command parameters from RESP array items
    /// Format: SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT timestamp | PXAT milliseconds-timestamp | KEEPTTL]
    fn parse(items: &[Value]) -> Option<Self> {
        // Minimum: SET key value
        if items.len() < 3 {
            return None;
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return None,
        };

        let value = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return None,
        };

        let mut params = SetParams::new(key, value);
        let mut i = 3;

        // Parse optional arguments
        while i < items.len() {
            let arg = match &items[i] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                Value::SimpleString(s) => s.to_uppercase(),
                _ => return None,
            };

            match arg.as_str() {
                "NX" => {
                    if params.mode.is_some() {
                        return None; // NX and XX are mutually exclusive
                    }
                    params.mode = Some(SetMode::Nx);
                    i += 1;
                }
                "XX" => {
                    if params.mode.is_some() {
                        return None; // NX and XX are mutually exclusive
                    }
                    params.mode = Some(SetMode::Xx);
                    i += 1;
                }
                "GET" => {
                    params.get = true;
                    i += 1;
                }
                "EX" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return None;
                    }
                    let seconds = parse_u64(&items[i + 1])?;
                    params.expiration = Some(Expiration::Ex(seconds));
                    i += 2;
                }
                "PX" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return None;
                    }
                    let milliseconds = parse_u64(&items[i + 1])?;
                    params.expiration = Some(Expiration::Px(milliseconds));
                    i += 2;
                }
                "EXAT" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return None;
                    }
                    let timestamp = parse_u64(&items[i + 1])?;
                    params.expiration = Some(Expiration::ExAt(timestamp));
                    i += 2;
                }
                "PXAT" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return None;
                    }
                    let timestamp = parse_u64(&items[i + 1])?;
                    params.expiration = Some(Expiration::PxAt(timestamp));
                    i += 2;
                }
                "KEEPTTL" => {
                    if params.expiration.is_some() {
                        return None;
                    }
                    params.expiration = Some(Expiration::KeepTTL);
                    i += 1;
                }
                _ => return None, // Unknown option
            }
        }

        Some(params)
    }
}

/// Parse a Value as u64
fn parse_u64(value: &Value) -> Option<u64> {
    match value {
        Value::BulkString(Some(data)) => String::from_utf8_lossy(data).parse::<u64>().ok(),
        Value::SimpleString(s) => s.parse::<u64>().ok(),
        Value::Integer(i) if *i >= 0 => Some(*i as u64),
        _ => None,
    }
}

/// SET command executor
pub struct SetCommand;

#[async_trait]
impl Command for SetCommand {
    async fn execute(&self, items: &[Value], server: &Server) -> Value {
        let params = match SetParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'set' command"),
        };

        let group = get_group_by_key(&server.app, &params.key);
        let get = params.get;
        match group.raft.client_write(Request::RedisSet(params)).await {
            Ok(res) => {
                if get {
                    res.data
                } else {
                    Value::ok()
                }
            }
            Err(e) => Value::error(format!("ERR {}", e)),
        }
    }
}
