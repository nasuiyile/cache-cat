//! PTTL command implementation
//!
//! PTTL key
//! Returns the remaining time to live of a key that has a timeout,
//! in milliseconds.
//!
//! Return value:
//! - Integer: TTL in milliseconds
//! - Integer: -1 if key exists but has no associated expire
//! - Integer: -2 if key does not exist

use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::EntrySnapshot;
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyValue;
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// PTTL command handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PTtlCommand;

/// Parsed arguments for PTTL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PTtlParams {
    pub key: Bytes,
}

impl Display for PTtlParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PttlParams {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

impl ReadCommand for PTtlParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, _value: Option<EntrySnapshot<MyValue>>) -> Value {
        // MyCache calls execute_with_clock; a raw value has no clock context.
        Value::error("ERR PTTL requires a logical clock")
    }

    fn execute_with_clock(&self, value: Option<EntrySnapshot<MyValue>>, now: u64) -> Value {
        match value {
            // Key does not exist
            None => Value::Integer(-2),

            Some(entry) => {
                match entry.expire_at {
                    // Key exists but has no associated expire
                    None => Value::Integer(-1),

                    // Key exists and has an expire time
                    Some(expire_at) => {
                        if now >= expire_at {
                            return Value::Integer(-2);
                        }
                        // Calculate remaining TTL in milliseconds
                        let ttl = expire_at.saturating_sub(now);
                        Value::Integer(ttl.min(i64::MAX as u64) as i64)
                    }
                }
            }
        }
    }
}

impl PTtlCommand {
    /// Parse PTTL arguments: PTTL key
    fn parse_args(items: &[Value]) -> Result<PTtlParams, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("pttl"));
        }

        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        Ok(PTtlParams { key })
    }
}

impl ReadRaftCommand for PTtlCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::PTtl(Self::parse_args(items)?))
    }
}

#[async_trait]
impl Command for PTtlCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(queue) = client.transaction_queue.as_mut() {
            queue.push(self.raft_request(items)?);
            return Ok(Value::queued());
        }

        let params = self.read_operation(items)?;
        server.app.read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::key::ttl::TtlParams;
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::string::get::GetParams;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::core::{MyCache, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::do_request;
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::request::{Operation, RedisOperation, Request};

    fn cache_with_ttl() -> MyCache {
        let cache = MyCache::new(1).unwrap();
        cache.set_write_clock(4_000);
        cache.databases[0].mocha.insert_absolute(
            "key".into(),
            MyValue::new(ValueObject::String("value".into())),
            10_000,
        );
        cache
    }

    #[test]
    fn ttl_reads_use_the_requested_clock_and_preserve_redis_replies() {
        let cache = cache_with_ttl();
        // Read-time expiry must not physically remove a key: it remains
        // available at the replicated clock after a later read clock hid it.
        for (clock, milliseconds, seconds) in [
            (4_000, 6_000, 6),
            (8_500, 1_500, 2),
            (8_501, 1_499, 1),
            (9_999, 1, 0),
            (10_000, -2, -2),
            (cache.get_write_clock(), 6_000, 6),
        ] {
            let pttl = cache.execute_read(PTtlParams { key: "key".into() }, 0, clock);
            let ttl = cache.execute_read(TtlParams { key: "key".into() }, 0, clock);
            assert_eq!(pttl.encode(), Value::Integer(milliseconds).encode());
            assert_eq!(ttl.encode(), Value::Integer(seconds).encode());
        }
        cache.databases[0].mocha.insert_persistent(
            "persistent".into(),
            MyValue::new(ValueObject::String("value".into())),
        );
        for (key, expected) in [("missing", -2), ("persistent", -1)] {
            let pttl = cache.execute_read(PTtlParams { key: key.into() }, 0, 4_000);
            let ttl = cache.execute_read(TtlParams { key: key.into() }, 0, 4_000);
            assert_eq!(pttl.encode(), Value::Integer(expected).encode());
            assert_eq!(ttl.encode(), Value::Integer(expected).encode());
        }
    }

    #[test]
    fn ttl_in_lua_and_exec_writes_deterministic_values_from_the_same_log() {
        let eval = Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
            "local ms = redis.call('PTTL', KEYS[1]); local s = redis.call('TTL', KEYS[1]); \
             redis.call('SET', KEYS[2], tostring(ms)); redis.call('SET', KEYS[3], tostring(s)); \
             return {ms, s}"
                .into(),
            3,
            vec!["key".into(), "milliseconds".into(), "seconds".into()],
            Vec::new(),
        )));
        let exec = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: vec![
                Operation::Read(ReadOperation::PTtl(PTtlParams { key: "key".into() })),
                Operation::Read(ReadOperation::Ttl(TtlParams { key: "key".into() })),
                eval.clone(),
            ],
        }));
        for (operation, expected) in [
            (eval, "*2\r\n:6000\r\n:6\r\n"),
            (exec, "*3\r\n:6000\r\n:6\r\n*2\r\n:6000\r\n:6\r\n"),
        ] {
            let log = bincode2::serialize(&Request::new(4_000, 0, operation)).unwrap();
            for replica in 0..2 {
                let cache = cache_with_ttl();
                if replica == 1 {
                    // Local reads may have advanced independently of this log.
                    cache.get_and_update_read_clock();
                }
                let request: Request = bincode2::deserialize(&log).unwrap();
                let (clock, db_number) = request.split_u64();
                let mut update_type = UpdateType::None;
                let mut update = Update {
                    db_number,
                    write_clock: cache.set_write_clock(clock),
                    update_type: &mut update_type,
                };
                let reply = do_request(&cache, request.operation, &mut update, true);
                assert_eq!(reply.encode(), expected.as_bytes());
                for (key, expected_value) in [
                    ("milliseconds", "$4\r\n6000\r\n"),
                    ("seconds", "$1\r\n6\r\n"),
                ] {
                    let saved = cache.execute_read(GetParams { key: key.into() }, 0, clock);
                    assert_eq!(saved.encode(), expected_value.as_bytes());
                }
            }
        }
    }
}
