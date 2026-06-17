use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::PExpire;
use crate::raft::types::entry::bae_operation::PExpireReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
    pub key: Bytes,
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

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let seconds = items[2].try_parse_u64()?;

        // Parse optional condition flag
        let condition = if items.len() >= 4 {
            let flag = items[3]
                .as_str_lossy()
                .ok_or(ProtocolError::WrongArgCount("expire"))?
                .to_uppercase();

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

/// EXPIRE command executor
pub struct ExpireCommand;

impl RaftCommand for ExpireCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = ExpireParams::parse(items)?;
        let req = PExpireReq {
            key: params.key,
            expires_at: params.seconds * 1000,
            condition: params.condition,
        };
        Ok(Operation::Base(PExpire(req)))
    }
}

#[async_trait]
impl Command for ExpireCommand {
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
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
