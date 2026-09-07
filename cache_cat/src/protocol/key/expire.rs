use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

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
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

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
        let req = ExpireReq {
            key: params.key,
            expires_at: params.seconds,
            condition: params.condition,
        };
        Ok(Operation::Base(req.into_base_op()))
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
            return Ok(Value::queued());
        }
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExpireReq {
    pub key: Bytes,
    pub expires_at: u64,
    pub condition: Option<ExpireCondition>,
}

impl Display for ExpireReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExpireReq {{ key: {}, seconds: {}, condition: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.expires_at,
            self.condition
        )
    }
}

impl ComputeCommand for ExpireReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::Expire(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expires_at = self.expires_at * 1000 + write_clock;
        let should_update = match self.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => entry.expire_at.is_none(),
                ExpireCondition::Xx => entry.expire_at.is_some(),
                ExpireCondition::Gt => {
                    match entry.expire_at {
                        None => false,                       // 无过期 = 无穷大，新过期不可能大于无穷大
                        Some(expire) => expire < expires_at, // 旧 < 新，即新 > 旧
                    }
                }
                ExpireCondition::Lt => {
                    match entry.expire_at {
                        None => true,                        // 无过期 = 无穷大，新过期一定小于无穷大
                        Some(expire) => expire > expires_at, // 旧 > 新，即新 < 旧
                    }
                }
            },
        };
        if !should_update {
            return (MochaOperation::Abort, Value::Boolean(false));
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: ExpirePolicy::Absolute(expires_at),
            },
            Value::Boolean(true),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Boolean(false))
    }
}
