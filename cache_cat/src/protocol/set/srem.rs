use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::SRem;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

struct SRemArgs {
    key: Bytes,
    members: Vec<Bytes>,
}

pub struct SRemCommand;

impl SRemCommand {
    fn parse_args(items: &[Value]) -> Result<SRemArgs, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("srem"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // Parse members
        let members = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if members.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("member"));
        }

        Ok(SRemArgs { key, members })
    }
}

impl RaftCommand for SRemCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(SRem(SRemReq {
            key: params.key,
            members: params.members,
        })))
    }
}

#[async_trait]
impl Command for SRemCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Transaction mode
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);

            return Ok(Value::queued());
        }

        // Build raft operation
        let operation = self.raft_request(items)?;

        // Execute write
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SRemReq {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl Display for SRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SRemReq {{ key: {}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.members
        )
    }
}

impl ComputeCommand for SRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        // Check if entry exists by checking if value is the default/empty state
        // This depends on how your EntrySnapshot works
        // Assuming entry.value exists but may be empty/placeholder

        match &entry.value.data {
            ValueObject::Set(set) => {
                let mut set = set.lock();
                let mut deleted_count = 0;
                for member in &self.members {
                    if set.remove(member) {
                        deleted_count += 1;
                    }
                }
                let is_empty = set.is_empty();
                drop(set);

                // Redis SREM returns the number of members that were removed
                if deleted_count == 0 {
                    // No members were removed, keep the set unchanged
                    return (
                        MochaOperation::Insert {
                            value: entry.value.clone(),
                            expire: entry.get_expire_policy(),
                        },
                        Value::Integer(0),
                    );
                }

                // Some members were removed
                if is_empty {
                    // Set is now empty, remove it entirely (Redis auto-removes empty sets)
                    return (MochaOperation::Remove, Value::Integer(deleted_count));
                }

                // Set still has members, update it
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(deleted_count),
                )
            }
            // Key exists but is not a Set - return error (Redis behavior)
            _ => (
                MochaOperation::Abort,
                ProtocolError::WrongType.into(),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        // For non-existent key, Redis SREM returns 0
        (MochaOperation::Abort, Value::Integer(0))
    }
}
