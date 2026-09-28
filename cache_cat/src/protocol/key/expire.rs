use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::base_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use crate::utils::checked_redis_deadline;
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
    /// A compatible combination of the flags above (for example XX GT).
    Multiple(Vec<ExpireCondition>),
}

impl ExpireCondition {
    pub(crate) fn allows(&self, current_expire: Option<u64>, new_expire: u64) -> bool {
        match self {
            Self::Nx => current_expire.is_none(),
            Self::Xx => current_expire.is_some(),
            Self::Gt => match current_expire {
                None => false,
                Some(expire) => expire < new_expire,
            },
            Self::Lt => match current_expire {
                None => true,
                Some(expire) => expire > new_expire,
            },
            Self::Multiple(conditions) => conditions
                .iter()
                .all(|condition| condition.allows(current_expire, new_expire)),
        }
    }
}

pub(crate) fn parse_expire_conditions(
    items: &[Value],
) -> Result<Option<ExpireCondition>, ProtocolError> {
    if items.len() <= 3 {
        return Ok(None);
    }

    let mut conditions = Vec::with_capacity(items.len() - 3);
    let mut has_nx = false;
    let mut has_xx = false;
    let mut has_gt = false;
    let mut has_lt = false;

    for item in &items[3..] {
        let flag = item
            .as_str_lossy()
            .ok_or(ProtocolError::SyntaxError)?
            .to_uppercase();
        let condition = match flag.as_str() {
            "NX" => {
                has_nx = true;
                ExpireCondition::Nx
            }
            "XX" => {
                has_xx = true;
                ExpireCondition::Xx
            }
            "GT" => {
                has_gt = true;
                ExpireCondition::Gt
            }
            "LT" => {
                has_lt = true;
                ExpireCondition::Lt
            }
            _ => {
                return Err(ProtocolError::response(format!(
                    "ERR Unsupported option {flag}"
                )));
            }
        };
        conditions.push(condition);
    }

    if (has_nx && (has_xx || has_gt || has_lt)) || (has_gt && has_lt) {
        if has_nx {
            return Err(ProtocolError::response(
                "ERR NX and XX, GT or LT options at the same time are not compatible",
            ));
        }
        return Err(ProtocolError::response(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }

    Ok(if conditions.len() == 1 {
        Some(conditions.pop().expect("one condition"))
    } else {
        Some(ExpireCondition::Multiple(conditions))
    })
}

/// EXPIRE command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct ExpireParams {
    pub key: Bytes,
    pub seconds: i64,
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

        let seconds = items[2].try_parse_canonical_i64()?;

        let condition = parse_expire_conditions(items)?;

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
        // Redis treats non-positive timeouts as immediate deletion. Only a
        // positive seconds value needs conversion to milliseconds, and only
        // that conversion can overflow.
        if params.seconds > 0 && params.seconds.checked_mul(1000).is_none() {
            return Err(ProtocolError::response(
                "ERR invalid expire time in 'expire' command",
            ));
        }
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
    pub expires_at: i64,
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
        // Redis treats a non-positive timeout as an immediate deletion.  Keep
        // the deadline at zero for that case so the condition checks below
        // still compare it as an earlier deadline than every live key.
        let expires_at = match self.checked_deadline(write_clock) {
            Ok(expires_at) => expires_at,
            Err(error) => return (MochaOperation::Abort, error.into()),
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

impl ExpireReq {
    pub(crate) fn checked_deadline(&self, write_clock: u64) -> Result<u64, ProtocolError> {
        if self.expires_at <= 0 {
            return Ok(0);
        }
        let milliseconds = (self.expires_at as u64).checked_mul(1000).ok_or_else(|| {
            ProtocolError::response("ERR invalid expire time in 'expire' command")
        })?;
        checked_redis_deadline(write_clock, milliseconds)
            .ok_or_else(|| ProtocolError::response("ERR invalid expire time in 'expire' command"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::raft_command::RaftCommand;
    use crate::raft::types::core::value_object::ValueObject;

    fn bulk(value: &'static str) -> Value {
        Value::BulkString(Some(Bytes::from_static(value.as_bytes())))
    }

    fn entry() -> EntrySnapshot<MyValue> {
        EntrySnapshot {
            value: MyValue::new(ValueObject::String(Bytes::from_static(b"value"))),
            expire_at: None,
        }
    }

    #[test]
    fn minimum_negative_timeout_is_accepted_as_immediate_expiration() {
        let args = [
            Value::BulkString(Some("EXPIRE".into())),
            Value::BulkString(Some("key".into())),
            Value::BulkString(Some(i64::MIN.to_string().into())),
        ];

        let params = ExpireParams::parse(&args).expect("canonical integer");
        assert_eq!(params.seconds, i64::MIN);
        assert!(ExpireCommand.raft_request(&args).is_ok());
    }

    #[test]
    fn negative_minimum_timeout_is_accepted_and_deletes_immediately() {
        let args = [bulk("EXPIRE"), bulk("key"), bulk("-9223372036854775808")];
        assert!(ExpireCommand.raft_request(&args).is_ok());

        let (operation, reply) = ExpireReq {
            key: Bytes::from_static(b"key"),
            expires_at: i64::MIN,
            condition: None,
        }
        .mutate(entry(), 123);
        assert!(matches!(operation, MochaOperation::Remove));
        assert_eq!(reply.encode(), b":1\r\n");
    }

    #[test]
    fn positive_seconds_overflow_is_rejected_before_replication() {
        let args = [bulk("EXPIRE"), bulk("key"), bulk("9223372036854776")];
        assert!(matches!(
            ExpireCommand.raft_request(&args),
            Err(ProtocolError::Response(_))
        ));
    }

    #[test]
    fn logical_clock_addition_overflow_aborts_without_mutating() {
        let (operation, reply) = ExpireReq {
            key: Bytes::from_static(b"key"),
            expires_at: 1,
            condition: None,
        }
        .mutate(entry(), u64::MAX);
        assert!(matches!(operation, MochaOperation::Abort));
        assert_eq!(
            reply.encode(),
            b"-ERR invalid expire time in 'expire' command\r\n"
        );
    }
}
