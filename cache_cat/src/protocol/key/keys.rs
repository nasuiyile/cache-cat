use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::{RaftCommand, ReadRaftCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::read_operation::ReadOperation;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// Parameters for KEYS command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysParams {
    pub pattern: Bytes,
}

impl Display for KeysParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KEYS {}", String::from_utf8_lossy(&self.pattern))
    }
}

impl KeysParams {
    pub fn execute(&self, cache: &MyCache, db_number: u16, read_clock: Option<u64>) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        Self::reply(database.mocha.keys(&self.pattern, read_clock))
    }

    /// Database-wide reads cannot use a single-key or a fixed-key-list trait.
    pub fn execute_with_clock(&self, cache: &MyCache, db_number: u16, read_clock: u64) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        let mut keys = database.mocha.keys(&self.pattern, Some(read_clock));
        keys.sort_unstable();
        Self::reply(keys)
    }

    fn reply(keys: Vec<Bytes>) -> Value {
        Value::Array(Some(
            keys.into_iter()
                .map(|key| Value::BulkString(Some(key)))
                .collect(),
        ))
    }

    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("keys"));
        }

        let pattern = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("pattern"))?;

        Ok(Self { pattern })
    }
}

/// KEYS command executor
pub struct KeysCommand;

impl ReadRaftCommand for KeysCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::Keys(KeysParams::parse(items)?))
    }
}

#[async_trait]
impl Command for KeysCommand {
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
        server.app.multi_read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::core::mocha::core::MyValue;
    use crate::raft::types::core::value_object::ValueObject;

    #[test]
    fn clocked_keys_sort_binary_names_and_filter_at_the_requested_clock() {
        let cache = MyCache::new(1).unwrap();
        cache.pause_expire_workers();
        cache.set_write_clock(100);
        let storage = &cache.databases[0].mocha;
        for key in [b"key:\xff".as_slice(), b"key:a", b"key:\0", b"other"] {
            storage.insert_persistent(
                Bytes::copy_from_slice(key),
                MyValue::new(ValueObject::String("v".into())),
            );
        }
        storage.insert_absolute(
            "key:expired".into(),
            MyValue::new(ValueObject::String("v".into())),
            200,
        );
        cache.get_and_update_read_clock();
        let params = KeysParams {
            pattern: "key:*".into(),
        };
        for (clock, names) in [
            (
                199,
                vec![b"key:\0".as_slice(), b"key:a", b"key:expired", b"key:\xff"],
            ),
            (200, vec![b"key:\0".as_slice(), b"key:a", b"key:\xff"]),
        ] {
            let expected =
                KeysParams::reply(names.into_iter().map(Bytes::copy_from_slice).collect());
            let reply = params.execute_with_clock(&cache, 0, clock);
            for proto in [2, 3] {
                assert_eq!(reply.encode_proto(proto), expected.encode_proto(proto));
            }
        }
        assert_eq!(storage.len(), 5, "read-time filtering must not delete keys");
        assert!(matches!(
            params.execute_with_clock(&cache, 1, 200),
            Value::Error(_)
        ));
    }
}
