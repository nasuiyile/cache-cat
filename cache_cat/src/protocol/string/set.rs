use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisSet;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

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
    pub key: Bytes,
    /// The value to set
    pub value: Bytes,
    /// NX or XX mode (optional)
    pub mode: Option<SetMode>,
    /// Whether to return the previous value (GET option)
    pub get: bool,
    /// Expiration time options (optional)
    pub expiration: Option<Expiration>,
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
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
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
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Minimum: SET key value
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("set"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let value = items[2]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("value"))?
            .clone();

        let mut params = SetParams::new(key, value);
        let mut i = 3;

        // Parse optional arguments
        while i < items.len() {
            let arg = items[i].as_str_lossy().ok_or(ProtocolError::SyntaxError)?;

            match arg.as_ref() {
                "NX" => {
                    if params.mode.is_some() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    params.mode = Some(SetMode::Nx);
                    i += 1;
                }
                "XX" => {
                    if params.mode.is_some() {
                        return Err(ProtocolError::SyntaxError);
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
                        return Err(ProtocolError::SyntaxError);
                    }
                    let seconds = items[i + 1].try_parse_u64()?;
                    params.expiration = Some(Expiration::Ex(seconds));
                    i += 2;
                }
                "PX" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    let milliseconds = items[i + 1].try_parse_u64()?;
                    params.expiration = Some(Expiration::Px(milliseconds));
                    i += 2;
                }
                "EXAT" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    let timestamp = items[i + 1].try_parse_u64()?;
                    params.expiration = Some(Expiration::ExAt(timestamp));
                    i += 2;
                }
                "PXAT" => {
                    if params.expiration.is_some() || i + 1 >= items.len() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    let timestamp = items[i + 1].try_parse_u64()?;
                    params.expiration = Some(Expiration::PxAt(timestamp));
                    i += 2;
                }
                "KEEPTTL" => {
                    if params.expiration.is_some() {
                        return Err(ProtocolError::SyntaxError);
                    }
                    params.expiration = Some(Expiration::KeepTTL);
                    i += 1;
                }
                _ => return Err(ProtocolError::SyntaxError),
            }
        }

        Ok(params)
    }
}

/// SET command executor
pub struct SetCommand;

impl RaftCommand for SetCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = SetParams::parse(items)?;
        Ok(Operation::Redis(RedisSet(params)))
    }
}

#[async_trait]
impl Command for SetCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }
        let params = SetParams::parse(items)?;
        let get = params.get;
        let value = server
            .app
            .write(Operation::Redis(RedisSet(params)), client.db_number)
            .await?;
        if get { Ok(value) } else { Ok(Value::ok()) }
    }
}
