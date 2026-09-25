use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::key::expire::{ExpireCondition, parse_expire_conditions};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::base_operation::BaseOperation::PExpire;
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
    pub milliseconds: i64,
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

        let milliseconds = items[2].try_parse_canonical_i64()?;

        let condition = parse_expire_conditions(items)?;

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
    pub expires_at: i64,
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
        // Redis treats a non-positive timeout as an immediate deletion.
        let expires_at = if self.expires_at <= 0 {
            0
        } else {
            write_clock.saturating_add(self.expires_at as u64)
        };
        let should_update = self.condition.as_ref().map_or(true, |condition| {
            condition.allows(entry.expire_at, expires_at)
        });
        if !should_update {
            return (MochaOperation::Abort, Value::Integer(0));
        }
        if expires_at <= write_clock {
            return (MochaOperation::Remove, Value::Integer(1));
        }
        (
            MochaOperation::Insert {
                value: entry.value.clone(),
                expire: ExpirePolicy::Absolute(expires_at),
            },
            Value::Integer(1),
        )
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}
