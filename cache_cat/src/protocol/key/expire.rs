use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::Expire;
use crate::raft::types::entry::bae_operation::ExpireReq;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Expire condition flags (NX, XX, GT, LT)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExpireCondition {
    /// NX - Only set expiration if key has NO existing expiration
    Nx,
    /// XX - Only set expiration if key already HAS an expiration
    Xx,
    /// GT - Only set expiration if new TTL is GREATER than current TTL
    Gt,
    /// LT - Only set expiration if new TTL is LESS than current TTL
    Lt,
}

/// EXPIRE command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct ExpireParams {
    pub key: Vec<u8>,
    pub seconds: u64,
    pub condition: Option<ExpireCondition>,
}

impl ExpireParams {
    /// Parse EXPIRE command parameters from RESP array items
    /// Format: EXPIRE key seconds [NX | XX | GT | LT]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: EXPIRE key seconds (3 items)
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("expire"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let seconds = parse_u64(&items[2]).ok_or(ProtocolError::NotAnInteger)?;

        // Parse optional condition flag
        let condition = if items.len() >= 4 {
            let flag = match &items[3] {
                Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
                Value::SimpleString(s) => s.to_uppercase(),
                _ => return Err(ProtocolError::WrongArgCount("expire")),
            };

            match flag.as_str() {
                "NX" => Some(ExpireCondition::Nx),
                "XX" => Some(ExpireCondition::Xx),
                "GT" => Some(ExpireCondition::Gt),
                "LT" => Some(ExpireCondition::Lt),
                _ => return Err(ProtocolError::SyntaxError),
            }
        } else {
            None
        };

        Ok(ExpireParams {
            key,
            seconds,
            condition,
        })
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

/// EXPIRE command executor
pub struct ExpireCommand;

#[async_trait]
impl Command for ExpireCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = ExpireParams::parse(items)?;
        let write_clock = server.app.state_machine.data.kvs.get_new_write_clock();
        let req = ExpireReq {
            key: Arc::from(params.key),
            expires_at: params.seconds * 1000 + write_clock,
            condition: params.condition,
        };
        let value = server.app.write_base(Expire(req), client.db_number).await?;

        Ok(value)
    }
}
