use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, ExpirePolicy, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::bloom_filter::{
    BLOOM_CAPACITY_MAX, BLOOM_CAPACITY_MIN, BLOOM_ERROR_RATE_CAP, BLOOM_EXPANSION_MAX,
    BLOOM_EXPANSION_MIN, BloomError, BloomObject, DEFAULT_BLOOM_EXPANSION,
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

/// BF.RESERVE key error_rate capacity
///     [EXPANSION expansion]
///     [NONSCALING]
#[derive(Debug, Clone, PartialEq)]
pub struct BfReserveParams {
    pub key: Bytes,

    /// Already capped to Redis BLOOM_ERROR_RATE_CAP.
    pub error_rate: f64,

    pub capacity: u64,

    pub expansion: u32,

    pub non_scaling: bool,
}

impl BfReserveParams {
    fn parse(values: &[Value]) -> Result<Self, ProtocolError> {
        /*
         * RedisBloom currently accepts argc:
         *
         * 4 ..= 7
         *
         * BF.RESERVE key error_rate capacity
         * BF.RESERVE key error_rate capacity NONSCALING
         * BF.RESERVE key error_rate capacity EXPANSION n
         * BF.RESERVE key error_rate capacity EXPANSION n NONSCALING
         *
         * The last form is normally an error when expansion > 0.
         */
        if values.len() < 4 || values.len() > 7 {
            return Err(ProtocolError::WrongArgCount("BF.RESERVE"));
        }

        let key = values[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        /*
         * error_rate
         */
        let error_bytes = values[2]
            .string_bytes_clone()
            .ok_or(ProtocolError::BloomBadErrorRate)?;

        let mut error_rate = parse_f64(&error_bytes).ok_or(ProtocolError::BloomBadErrorRate)?;

        /*
         * Redis config says valid range is:
         *
         * (0, 1)
         *
         * NaN/Infinity should not be accepted.
         */
        if !error_rate.is_finite() {
            return Err(ProtocolError::BloomBadErrorRate);
        }

        if error_rate <= 0.0 || error_rate >= 1.0 {
            return Err(ProtocolError::BloomErrorRateOutOfRange);
        }

        /*
         * Important Redis behavior:
         *
         * BF.RESERVE x 0.9 100
         *
         * succeeds, but the actual error rate used is 0.25.
         */
        if error_rate > BLOOM_ERROR_RATE_CAP {
            error_rate = BLOOM_ERROR_RATE_CAP;
        }

        /*
         * capacity
         */
        let capacity_bytes = values[3]
            .string_bytes_clone()
            .ok_or(ProtocolError::BloomBadCapacity)?;

        let capacity = parse_i64(&capacity_bytes).ok_or(ProtocolError::BloomBadCapacity)?;

        if capacity < BLOOM_CAPACITY_MIN as i64 || capacity > BLOOM_CAPACITY_MAX as i64 {
            return Err(ProtocolError::BloomCapacityOutOfRange);
        }

        let capacity = capacity as u64;

        /*
         * Redis default:
         *
         * expansion = 2
         * scaling   = enabled
         */
        let mut expansion = DEFAULT_BLOOM_EXPANSION;

        let mut expansion_was_set = false;

        let mut non_scaling = false;

        /*
         * Parse optional arguments.
         *
         * Redis command keywords are case-insensitive.
         */
        let mut index = 4;

        while index < values.len() {
            let option = values[index]
                .string_bytes_clone()
                .ok_or(ProtocolError::SyntaxError)?;

            if option.as_ref().eq_ignore_ascii_case(b"NONSCALING") {
                /*
                 * Duplicate NONSCALING isn't useful.
                 * Treat it as syntax error rather than
                 * producing ambiguous state.
                 */
                if non_scaling {
                    return Err(ProtocolError::SyntaxError);
                }

                non_scaling = true;

                index += 1;

                continue;
            }

            if option.as_ref().eq_ignore_ascii_case(b"EXPANSION") {
                if expansion_was_set {
                    return Err(ProtocolError::SyntaxError);
                }
                /*
                 * EXPANSION must have a value.
                 */
                if index + 1 >= values.len() {
                    return Err(ProtocolError::BloomNoExpansion);
                }
                let expansion_bytes = values[index + 1]
                    .string_bytes_clone()
                    .ok_or(ProtocolError::BloomBadExpansion)?;

                let parsed_expansion =
                    parse_i64(&expansion_bytes).ok_or(ProtocolError::BloomBadExpansion)?;

                if parsed_expansion < BLOOM_EXPANSION_MIN as i64
                    || parsed_expansion > BLOOM_EXPANSION_MAX as i64
                {
                    return Err(ProtocolError::BloomExpansionOutOfRange);
                }
                expansion = parsed_expansion as u32;
                expansion_was_set = true;
                index += 2;
                continue;
            }

            return Err(ProtocolError::SyntaxError);
        }

        /*
         * RedisBloom implementation detail:
         *
         * EXPANSION 0
         *
         * is accepted and effectively means NONSCALING.
         *
         * Although the public BF.RESERVE documentation describes
         * EXPANSION as positive, current Redis configuration permits
         * [0, 32768], and its command implementation maps 0 to
         * BLOOM_OPT_NO_SCALING.
         */
        if expansion == 0 {
            non_scaling = true;
        } else if non_scaling && expansion_was_set {
            /*
             * Example:
             *
             * BF.RESERVE bf 0.01 100
             *     EXPANSION 2 NONSCALING
             *
             * Redis:
             *
             * Nonscaling filters cannot expand
             */
            return Err(ProtocolError::BloomNonScalingCannotExpand);
        }

        Ok(Self {
            key,
            error_rate,
            capacity,
            expansion,
            non_scaling,
        })
    }
}

pub struct BfReserveCommand;

impl RaftCommand for BfReserveCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = BfReserveParams::parse(items)?;

        /*
         * Important for Raft:
         *
         * Put the resolved/capped values into the log.
         *
         * Followers must NOT re-read a local Redis config
         * during state-machine apply.
         */
        Ok(Operation::Base(BaseOperation::BfReserve(BfReserveReq {
            key: params.key,
            error_rate: params.error_rate,
            capacity: params.capacity,
            expansion: params.expansion,
            non_scaling: params.non_scaling,
        })))
    }
}

#[async_trait]
impl Command for BfReserveCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        /*
         * MULTI / EXEC:
         *
         * Parse before queueing, just like your other commands.
         */
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);

            return Ok(Value::SimpleString("QUEUED".to_string()));
        }

        let operation = self.raft_request(items)?;

        server.app.write(operation, client.db_number).await
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BfReserveReq {
    pub key: Bytes,

    pub error_rate: f64,

    pub capacity: u64,

    pub expansion: u32,

    pub non_scaling: bool,
}

impl fmt::Display for BfReserveReq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BfReserveReq {{ key: {}, error_rate: {}, capacity: {}, expansion: {}, non_scaling: {} }}",
            String::from_utf8_lossy(&self.key),
            self.error_rate,
            self.capacity,
            self.expansion,
            self.non_scaling,
        )
    }
}

impl ComputeCommand for BfReserveReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::BfReserve(self)
    }

    /*
     * mutate() means:
     *
     * key already exists.
     *
     * BF.RESERVE NEVER overwrites an existing key.
     */
    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            /*
             * Existing Bloom filter:
             *
             * Redis:
             * ERR item exists
             */
            ValueObject::Bloom(_) => (MochaOperation::Abort, ProtocolError::BloomItemExists.into()),

            /*
             * Existing STRING / HASH / LIST / etc:
             *
             * Redis:
             * WRONGTYPE ...
             */
            _ => (MochaOperation::Abort, ProtocolError::WrongType.into()),
        }
    }

    /*
     * init() means key doesn't exist.
     *
     * BF.RESERVE creates an EMPTY filter.
     */
    fn init(self) -> (MochaOperation<MyValue>, Value) {
        let bloom = match BloomObject::new(
            self.capacity,
            self.error_rate,
            self.expansion,
            self.non_scaling,
        ) {
            Ok(bloom) => bloom,

            Err(error) => {
                return (MochaOperation::Abort, bloom_create_error(error).into());
            }
        };

        (
            MochaOperation::Insert {
                value: MyValue::new(ValueObject::Bloom(Arc::new(Mutex::new(bloom)))),

                /*
                 * BF.RESERVE creates a new persistent key.
                 */
                expire: ExpirePolicy::Persistent,
            },
            Value::SimpleString("OK".to_string()),
        )
    }
}

#[inline]
fn bloom_create_error(error: BloomError) -> ProtocolError {
    match error {
        BloomError::OutOfMemory => ProtocolError::BloomCreateOutOfMemory,

        BloomError::Full | BloomError::Invalid | BloomError::Overflow => {
            ProtocolError::BloomCreateFailed
        }
    }
}

#[inline]
fn parse_f64(bytes: &[u8]) -> Option<f64> {
    let value = std::str::from_utf8(bytes).ok()?;

    value.parse::<f64>().ok()
}
