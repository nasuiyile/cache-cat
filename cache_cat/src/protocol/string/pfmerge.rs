use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::MultiReadComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::structure::hll::{HllDecodeError, RedisHll};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

const WRONG_HLL_TYPE: &str = "WRONGTYPE Key is not a valid HyperLogLog string value.";
const CORRUPTED_HLL: &str = "INVALIDOBJ Corrupted HLL object detected";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PFMergeParams {
    pub key: Bytes,
    pub keys: Vec<Bytes>,
}

impl PFMergeParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("PFMERGE"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let keys = items
            .iter()
            .skip(1)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if keys.len() != items.len() - 1 {
            return Err(ProtocolError::InvalidArgument("key"));
        }

        Ok(Self { key, keys })
    }
}

impl Display for PFMergeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PFMERGE")?;

        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }

        Ok(())
    }
}

pub struct PFMergeCommand;

impl RaftCommand for PFMergeCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = PFMergeParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::PFMerge(PFMergeReq {
            key: params.key,
            keys: params.keys,
        })))
    }
}

#[async_trait]
impl Command for PFMergeCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PFMergeReq {
    pub key: Bytes,
    /// Destination key followed by all source keys.
    pub keys: Vec<Bytes>,
}

impl Display for PFMergeReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PFMERGE")?;

        for key in &self.keys {
            write!(f, " {}", String::from_utf8_lossy(key))?;
        }

        Ok(())
    }
}

impl MultiReadComputeCommand for PFMergeReq {
    fn write_key(&self) -> &Bytes {
        &self.key
    }

    fn read_keys(&self) -> &[Bytes] {
        &self.keys
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::PFMerge(self)
    }

    fn mutate(
        self,
        read_entries: Vec<Option<EntrySnapshot<MyValue>>>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        debug_assert_eq!(read_entries.len(), self.keys.len());

        // PFMERGE preserves the destination TTL.
        let expire = read_entries
            .first()
            .and_then(|entry| entry.as_ref())
            .map(|snapshot| snapshot.get_expire_policy())
            .unwrap_or(ExpirePolicy::Persistent);

        let mut merged = RedisHll::new();

        for (index, entry) in read_entries.into_iter().enumerate() {
            let Some(snapshot) = entry else {
                continue;
            };

            let hll = match snapshot.value.data {
                ValueObject::String(raw) => match RedisHll::decode(raw.as_ref()) {
                    Ok(hll) => hll,
                    Err(HllDecodeError::NotHll) => {
                        return (MochaOperation::Abort, invalid_hll());
                    }
                    Err(HllDecodeError::Corrupted) => {
                        return (MochaOperation::Abort, corrupted_hll());
                    }
                },
                ValueObject::Int(_) => {
                    return (MochaOperation::Abort, invalid_hll());
                }
                _ => {
                    return (
                        MochaOperation::Abort,
                        CacheCatError::from(ProtocolError::WrongType).into(),
                    );
                }
            };

            if index == 0 {
                merged = hll;
            } else {
                merged.merge(&hll);
            }
        }
        // Redis always invalidates the cached cardinality on PFMERGE.
        merged.invalidate_cache();

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::String(merged.into_bytes())),
                expire,
            },
            Value::SimpleString(String::from("OK")),
        )
    }
}

#[inline]
fn invalid_hll() -> Value {
    ProtocolError::response(WRONG_HLL_TYPE).into()
}

#[inline]
fn corrupted_hll() -> Value {
    ProtocolError::response(CORRUPTED_HLL).into()
}
