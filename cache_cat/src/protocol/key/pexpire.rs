use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::key::expire::ExpireCondition;
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::PExpire;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

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
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

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

            return Ok(Value::queued());
        }
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PExpireReq {
    pub key: Bytes,
    pub expires_at: u64,
    pub condition: Option<ExpireCondition>,
}

impl Display for PExpireReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExpireReq {{ key: {}, milliseconds: {}, condition: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.expires_at,
            self.condition
        )
    }
}

impl ComputeCommand for PExpireReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::PExpire(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expires_at = self.expires_at + write_clock;
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
