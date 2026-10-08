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

/// Parameters for DBSIZE command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbsizeParams {
    pub keys: Vec<Bytes>,
}

impl Display for DbsizeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DBSIZE")
    }
}

impl DbsizeParams {
    /// Count logical live keys rather than replica-local expiry worker progress.
    /// Only replicated execution pays for this scan; ordinary DBSIZE stays O(1).
    pub fn execute_with_clock(&self, cache: &MyCache, db_number: u16, read_clock: u64) -> Value {
        let database = match cache.get_cache(db_number) {
            Ok(database) => database,
            Err(error) => return error,
        };
        Value::Integer(database.mocha.len_with_read_clock(read_clock) as i64)
    }

    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        if items.len() != 1 {
            return Err(ProtocolError::WrongArgCount("dbsize"));
        }

        Ok(Self { keys: Vec::new() })
    }
}

/// DBSIZE command executor
pub struct DbsizeCommand;

impl ReadRaftCommand for DbsizeCommand {
    fn read_operation(&self, items: &[Value]) -> Result<ReadOperation, ProtocolError> {
        Ok(ReadOperation::DbSize(DbsizeParams::parse(items)?))
    }
}

#[async_trait]
impl Command for DbsizeCommand {
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

        let params = self.read_operation(items)?;
        server.app.multi_read(params, client.db_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::lua::eval::EvalParams;
    use crate::protocol::string::get::GetParams;
    use crate::protocol::transaction::QueuedOperation;
    use crate::protocol::transaction::exec::ExecParams;
    use crate::raft::types::core::mocha::core::{MyValue, Update, UpdateType};
    use crate::raft::types::core::mocha::request_handler::{do_request, read_request};
    use crate::raft::types::core::value_object::ValueObject;
    use crate::raft::types::entry::request::{Operation, RedisOperation, Request};

    #[test]
    fn replicated_dbsize_ignores_physical_expiration_progress() {
        let replicas = [MyCache::new(1).unwrap(), MyCache::new(1).unwrap()];
        for cache in &replicas {
            cache.pause_expire_workers();
            cache.set_write_clock(100);
            let value = MyValue::new(ValueObject::String("v".into()));
            let storage = &cache.databases[0].mocha;
            storage.insert_persistent("persistent".into(), value.clone());
            storage.insert_absolute("expired".into(), value.clone(), 200);
            storage.insert_absolute("boundary".into(), value.clone(), 300);
            storage.insert_absolute("future".into(), value, 301);
            cache.set_write_clock(300);
        }
        // Only one replica has physically removed keys invisible at this log.
        replicas[1].resume_expire_workers();
        replicas[1].databases[0]
            .mocha
            .active_expire_cycle_blocking();
        assert_eq!(replicas[0].databases[0].mocha.len(), 4);
        assert_eq!(replicas[1].databases[0].mocha.len(), 2);
        replicas[1].get_and_update_read_clock();

        let params = DbsizeParams { keys: Vec::new() };
        let operation = Operation::Redis(RedisOperation::RedisExec(ExecParams {
            operations: vec![
                QueuedOperation::new(0, Operation::Read(ReadOperation::DbSize(params.clone()))),
                QueuedOperation::new(
                    0,
                    Operation::Redis(RedisOperation::RedisEval(EvalParams::new(
                        "local n = redis.call('DBSIZE'); redis.call('SET', KEYS[1], tostring(n)); return n".into(),
                        1,
                        vec!["saved-size".into()],
                        Vec::new(),
                    ))),
                ),
            ],
        }));
        let log = bincode2::serialize(&Request::new(300, 0, operation)).unwrap();
        for (replica, cache) in replicas.iter().enumerate() {
            // The ordinary read path still returns the O(1) physical count.
            assert_eq!(
                read_request(cache, ReadOperation::DbSize(params.clone()), 0, 300).encode(),
                Value::Integer(if replica == 0 { 4 } else { 2 }).encode()
            );
            let request: Request = bincode2::deserialize(&log).unwrap();
            let (clock, db_number) = request.split_u64();
            let mut update_type = UpdateType::None;
            let mut update = Update {
                db_number,
                write_clock: cache.set_write_clock(clock),
                update_type: &mut update_type,
            };
            let reply = do_request(cache, request.operation, &mut update, true);
            assert_eq!(reply.encode(), b"*2\r\n:2\r\n:2\r\n");
            let saved = cache.execute_read(
                GetParams {
                    key: "saved-size".into(),
                },
                0,
                clock,
            );
            assert_eq!(saved.encode(), b"$1\r\n2\r\n");
        }
    }
}
