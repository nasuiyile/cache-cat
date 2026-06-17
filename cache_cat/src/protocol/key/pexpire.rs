use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::key::expire::ExpireCondition;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::PExpire;
use crate::raft::types::entry::bae_operation::PExpireReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

/// PEXPIRE command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct PExpireParams {
    pub key: Bytes,
    pub milliseconds: u64,
    pub condition: Option<ExpireCondition>,
}

impl PExpireParams {
    /// Parse PEXPIRE command parameters from RESP array items
    ///
    /// Format:
    /// PEXPIRE key milliseconds [NX | XX | GT | LT]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: PEXPIRE key milliseconds
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("pexpire"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let milliseconds = items[2].try_parse_u64()?;

        let condition = if items.len() >= 4 {
            let flag = items[3]
                .as_str_lossy()
                .ok_or(ProtocolError::WrongArgCount("pexpire"))?
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

        Ok(PExpireParams {
            key,
            milliseconds,
            condition,
        })
    }
}

/// PEXPIRE command executor
pub struct PExpireCommand;

impl RaftCommand for PExpireCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = PExpireParams::parse(items)?;
        let req = PExpireReq {
            key: params.key,
            expires_at: params.milliseconds,
            condition: params.condition,
        };
        Ok(Operation::Base(PExpire(req)))
    }
}

#[async_trait]
impl Command for PExpireCommand {
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
