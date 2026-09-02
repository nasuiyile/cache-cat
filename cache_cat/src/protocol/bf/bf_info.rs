use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::{Resp2MapEncoding, Value};
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::read_operation::ReadOperation;

use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// BF.INFO optional information selector.
///
/// BF.INFO key
/// BF.INFO key CAPACITY
/// BF.INFO key SIZE
/// BF.INFO key FILTERS
/// BF.INFO key ITEMS
/// BF.INFO key EXPANSION
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BfInfoField {
    Capacity,
    Size,
    Filters,
    Items,
    Expansion,
}

impl BfInfoField {
    fn parse(value: &[u8]) -> Result<Self, ProtocolError> {
        if value.eq_ignore_ascii_case(b"CAPACITY") {
            return Ok(Self::Capacity);
        }

        if value.eq_ignore_ascii_case(b"SIZE") {
            return Ok(Self::Size);
        }

        if value.eq_ignore_ascii_case(b"FILTERS") {
            return Ok(Self::Filters);
        }

        if value.eq_ignore_ascii_case(b"ITEMS") {
            return Ok(Self::Items);
        }

        if value.eq_ignore_ascii_case(b"EXPANSION") {
            return Ok(Self::Expansion);
        }

        Err(ProtocolError::BloomInvalidInformationValue)
    }

    fn redis_name(self) -> &'static str {
        match self {
            Self::Capacity => "Capacity",

            Self::Size => "Size",

            Self::Filters => "Number of filters",

            Self::Items => "Number of items inserted",

            Self::Expansion => "Expansion rate",
        }
    }
}

/// Parameters for BF.INFO command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BfInfoParams {
    pub key: Bytes,

    pub field: Option<BfInfoField>,
}

impl Display for BfInfoParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BF.INFO {}", String::from_utf8_lossy(&self.key))?;

        if let Some(field) = self.field {
            write!(
                f,
                " {}",
                match field {
                    BfInfoField::Capacity => "CAPACITY",

                    BfInfoField::Size => "SIZE",

                    BfInfoField::Filters => "FILTERS",

                    BfInfoField::Items => "ITEMS",

                    BfInfoField::Expansion => "EXPANSION",
                }
            )?;
        }

        Ok(())
    }
}

impl BfInfoParams {
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 && items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("BF.INFO"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        let field = if items.len() == 3 {
            let value = items[2]
                .string_bytes_clone()
                .ok_or(ProtocolError::BloomInvalidInformationValue)?;

            Some(BfInfoField::parse(value.as_ref())?)
        } else {
            None
        };

        Ok(Self { key, field })
    }
}

/// BF.INFO command executor.
pub struct BfInfoCommand;

impl ReadCommand for BfInfoParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<EntrySnapshot<MyValue>>) -> Value {
        let entry = match value {
            Some(entry) => entry,
            None => return ProtocolError::BloomNotFound.into(),
        };

        let bloom = match &entry.value.data {
            ValueObject::Bloom(bloom) => bloom,
            _ => return ProtocolError::WrongType.into(),
        };

        let bloom = bloom.lock();

        match self.field {
            None => Value::Map(vec![
                info_entry(BfInfoField::Capacity, &bloom),
                info_entry(BfInfoField::Size, &bloom),
                info_entry(BfInfoField::Filters, &bloom),
                info_entry(BfInfoField::Items, &bloom),
                info_entry(BfInfoField::Expansion, &bloom),
            ]),

            Some(field) => Value::MapWithResp2 {
                entries: vec![info_entry(field, &bloom)],
                resp2: Resp2MapEncoding::Values,
            },
        }
    }
}

fn info_entry(field: BfInfoField, bloom: &BloomObject) -> (Value, Value) {
    (
        Value::SimpleString(field.redis_name().to_string()),
        info_value(field, bloom),
    )
}

fn info_value(field: BfInfoField, bloom: &BloomObject) -> Value {
    match field {
        BfInfoField::Capacity => Value::Integer(u64_to_redis_integer(bloom.info_capacity())),

        BfInfoField::Size => Value::Integer(usize_to_redis_integer(bloom.info_size())),

        BfInfoField::Filters => Value::Integer(usize_to_redis_integer(bloom.info_filter_count())),

        BfInfoField::Items => Value::Integer(u64_to_redis_integer(bloom.info_items())),

        BfInfoField::Expansion => {
            match bloom.info_expansion() {
                Some(expansion) => Value::Integer(i64::from(expansion)),

                /*
                 * Redis current implementation:
                 *
                 * NONSCALING =>
                 * Expansion rate = Null
                 */
                None => Value::Null,
            }
        }
    }
}

#[inline]
fn u64_to_redis_integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[inline]
fn usize_to_redis_integer(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl ReadRaftCommand for BfInfoCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::BfInfo(BfInfoParams::parse(items)?))
    }
}

#[async_trait]
impl Command for BfInfoCommand {
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

        let operation = self.read_operation(items)?;

        server.app.read(operation, client.db_number).await
    }
}
