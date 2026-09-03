use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::bloom_filter::{
    BloomError, BloomObject, BLOOM_CAPACITY_MAX, BLOOM_CAPACITY_MIN, BLOOM_ERROR_RATE_CAP,
    BLOOM_EXPANSION_MAX, BLOOM_EXPANSION_MIN, DEFAULT_BLOOM_CAPACITY,
    DEFAULT_BLOOM_ERROR_RATE, DEFAULT_BLOOM_EXPANSION,
};
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::Operation;
use crate::utils::parse_i64;
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct BfInsertParams {
    pub key: Bytes,
    pub items: Vec<Bytes>,
    pub capacity: u64,
    pub error_rate: f64,
    pub expansion: u32,
    pub autocreate: bool,
    pub non_scaling: bool,
}

impl BfInsertParams {
    fn parse(values: &[Value]) -> Result<Self, ProtocolError> {
        if values.len() < 4 {
            return Err(ProtocolError::WrongArgCount("BF.INSERT"));
        }

        let key = values[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let mut capacity = DEFAULT_BLOOM_CAPACITY;
        let mut error_rate = DEFAULT_BLOOM_ERROR_RATE;
        let mut expansion = DEFAULT_BLOOM_EXPANSION;
        let mut autocreate = true;
        let mut non_scaling = false;

        let mut index = 2;
        let mut items_index = None;

        while index < values.len() {
            let option = values[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::BloomInsertUnknownArgument)?;

            if option.as_ref().eq_ignore_ascii_case(b"ITEMS") {
                items_index = Some(index + 1);
                break;
            }

            if option.as_ref().eq_ignore_ascii_case(b"ERROR") {
                index += 1;

                if index >= values.len() {
                    return Err(ProtocolError::WrongArgCount("BF.INSERT"));
                }

                let value = values[index]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::BloomInsertBadErrorRate)?;

                error_rate = parse_f64(&value)
                    .filter(|v| v.is_finite())
                    .ok_or(ProtocolError::BloomInsertBadErrorRate)?;

                if error_rate <= 0.0 || error_rate >= 1.0 {
                    return Err(ProtocolError::BloomInsertBadErrorRate);
                }

                if error_rate > BLOOM_ERROR_RATE_CAP {
                    error_rate = BLOOM_ERROR_RATE_CAP;
                }

                index += 1;
                continue;
            }

            if option.as_ref().eq_ignore_ascii_case(b"CAPACITY") {
                index += 1;

                if index >= values.len() {
                    return Err(ProtocolError::WrongArgCount("BF.INSERT"));
                }

                let value = values[index]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::BloomInsertBadCapacity)?;

                let parsed =
                    parse_i64(&value).ok_or(ProtocolError::BloomInsertBadCapacity)?;

                if parsed < BLOOM_CAPACITY_MIN as i64
                    || parsed > BLOOM_CAPACITY_MAX as i64
                {
                    return Err(ProtocolError::BloomInsertBadCapacity);
                }

                capacity = parsed as u64;

                index += 1;
                continue;
            }

            if option.as_ref().eq_ignore_ascii_case(b"EXPANSION") {
                index += 1;

                if index >= values.len() {
                    return Err(ProtocolError::WrongArgCount("BF.INSERT"));
                }

                let value = values[index]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::BloomInsertBadExpansion)?;

                let parsed =
                    parse_i64(&value).ok_or(ProtocolError::BloomInsertBadExpansion)?;

                if parsed < BLOOM_EXPANSION_MIN as i64
                    || parsed > BLOOM_EXPANSION_MAX as i64
                {
                    return Err(ProtocolError::BloomInsertBadExpansion);
                }

                expansion = parsed as u32;

                index += 1;
                continue;
            }

            if option.as_ref().eq_ignore_ascii_case(b"NOCREATE") {
                autocreate = false;
                index += 1;
                continue;
            }

            if option.as_ref().eq_ignore_ascii_case(b"NONSCALING") {
                non_scaling = true;
                index += 1;
                continue;
            }

            return Err(ProtocolError::BloomInsertUnknownArgument);
        }

        let items_index =
            items_index.ok_or(ProtocolError::WrongArgCount("BF.INSERT"))?;

        if items_index >= values.len() {
            return Err(ProtocolError::WrongArgCount("BF.INSERT"));
        }

        if expansion == 0 {
            non_scaling = true;
        }

        let mut items = Vec::with_capacity(values.len() - items_index);

        for value in &values[items_index..] {
            let item = value
                .string_bytes_clone()
                .ok_or(ProtocolError::InvalidArgument("item"))?;

            items.push(item);
        }

        Ok(Self {
            key,
            items,
            capacity,
            error_rate,
            expansion,
            autocreate,
            non_scaling,
        })
    }
}

pub struct BfInsertCommand;

impl RaftCommand for BfInsertCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BfInsertParams::parse(items)?;

        Ok(Operation::Base(BaseOperation::BfInsert(BfInsertReq {
            key: params.key,
            items: params.items,
            capacity: params.capacity,
            error_rate: params.error_rate,
            expansion: params.expansion,
            autocreate: params.autocreate,
            non_scaling: params.non_scaling,
        })))
    }
}

#[async_trait]
impl Command for BfInsertCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::SimpleString("QUEUED".to_string()));
        }

        let operation = self.raft_request(items)?;

        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BfInsertReq {
    pub key: Bytes,
    pub items: Vec<Bytes>,
    pub capacity: u64,
    pub error_rate: f64,
    pub expansion: u32,
    pub autocreate: bool,
    pub non_scaling: bool,
}

impl fmt::Display for BfInsertReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BfInsertReq {{ key: {}, items: {}, capacity: {}, error_rate: {}, expansion: {}, autocreate: {}, non_scaling: {} }}",
            String::from_utf8_lossy(&self.key),
            self.items.len(),
            self.capacity,
            self.error_rate,
            self.expansion,
            self.autocreate,
            self.non_scaling,
        )
    }
}

impl ComputeCommand for BfInsertReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BfInsert(self)
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        let expire = entry.get_expire_policy();

        let (results, mutated) = {
            let bloom = match &entry.value.data {
                ValueObject::Bloom(bloom) => bloom,
                _ => {
                    return (
                        MochaOperation::Abort,
                        ProtocolError::WrongType.into(),
                    );
                }
            };

            let mut bloom = bloom.lock();
            add_items(&mut bloom, &self.items)
        };

        let reply = Value::Array(Some(results));

        if mutated {
            (
                MochaOperation::Insert {
                    value: entry.value,
                    expire,
                },
                reply,
            )
        } else {
            (MochaOperation::Abort, reply)
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        if !self.autocreate {
            return (
                MochaOperation::Abort,
                ProtocolError::BloomNotFound.into(),
            );
        }

        let mut bloom = match BloomObject::new(
            self.capacity,
            self.error_rate,
            self.expansion,
            self.non_scaling,
        ) {
            Ok(bloom) => bloom,
            Err(error) => {
                return (
                    MochaOperation::Abort,
                    bloom_create_error(error).into(),
                );
            }
        };

        let (results, _) = add_items(&mut bloom, &self.items);

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(
                    bloom,
                )))),
                expire: ExpirePolicy::Persistent,
            },
            Value::Array(Some(results)),
        )
    }
}

fn add_items(bloom: &mut BloomObject, items: &[Bytes]) -> (Vec<Value>, bool) {
    let mut results = Vec::with_capacity(items.len());
    let mut mutated = false;

    for item in items {
        match bloom.add(item) {
            Ok(true) => {
                mutated = true;
                results.push(Value::Boolean(true));
            }

            Ok(false) => {
                results.push(Value::Boolean(false));
            }

            Err(BloomError::Full) => {
                results.push(ProtocolError::BloomFilterFull.into());
                break;
            }

            Err(
                BloomError::OutOfMemory
                | BloomError::Invalid
                | BloomError::Overflow,
            ) => {
                results.push(ProtocolError::BloomInsertFailed.into());
            }
        }
    }

    (results, mutated)
}

fn bloom_create_error(error: BloomError) -> ProtocolError {
    match error {
        BloomError::OutOfMemory => ProtocolError::BloomCreateOutOfMemory,
        BloomError::Full | BloomError::Invalid | BloomError::Overflow => {
            ProtocolError::BloomCreateFailed
        }
    }
}

fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}