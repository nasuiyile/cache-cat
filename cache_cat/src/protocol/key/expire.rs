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
        if params.seconds.checked_mul(1000).is_none() {
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
        let expires_at = if self.expires_at <= 0 {
            0
        } else {
            let milliseconds = (self.expires_at as u64)
                .checked_mul(1000)
                .unwrap_or(u64::MAX);
            write_clock.saturating_add(milliseconds)
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
