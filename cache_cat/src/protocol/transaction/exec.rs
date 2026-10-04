use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::transaction::QueuedOperation;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::core::MyCache;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecParams {
    pub operations: Vec<QueuedOperation>,
}

impl ExecParams {
    pub(crate) fn resolve_scripts(&mut self, cache: &MyCache) {
        // Acquire only for EVALSHA and keep one cache view for the whole EXEC.
        let mut scripts = None;
        for queued in &mut self.operations {
            let operation = &mut queued.operation;
            if matches!(
                operation,
                Operation::Redis(RedisOperation::RedisEvalSha { .. })
            ) {
                let Operation::Redis(RedisOperation::RedisEvalSha { params, proto }) =
                    std::mem::replace(
                        operation,
                        Operation::Redis(RedisOperation::RedisReply(Value::ok())),
                    )
                else {
                    unreachable!();
                };
                // Capture source or NOSCRIPT in the log so all replicas agree.
                let scripts = scripts.get_or_insert_with(|| cache.lua_env.script_map.lock());
                *operation = params
                    .into_operation(scripts, proto)
                    .unwrap_or_else(|error| {
                        Operation::Redis(RedisOperation::RedisReply(error.into()))
                    });
            }
        }
    }
}
impl Display for ExecParams {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub struct ExecCommand;

#[async_trait]
impl Command for ExecCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("EXEC").into());
        }
        // If no transaction has been initiated
        let queue = client
            .transaction_queue
            .take()
            .ok_or(ProtocolError::response("ERR EXEC without MULTI"))?;

        // EXEC always ends MULTI, including the abort path. A queue-time
        // validation error discards every queued operation and does not enter
        // the Raft state machine.
        let transaction_failed = client.transaction_failed;
        client.transaction_failed = false;
        client.flag.multi = false;
        if transaction_failed {
            return Err(ProtocolError::response(
                "EXECABORT Transaction discarded because of previous errors.",
            )
            .into());
        }

        let final_db = queue.db_number;
        let mut params = ExecParams {
            operations: queue.operations,
        };
        params.resolve_scripts(&server.app.state_machine.data.kvs);

        let value = server
            .app
            .write(
                Operation::Redis(RedisOperation::RedisExec(params)),
                client.db_number,
            )
            .await?;
        client.db_number = final_db;
        Ok(value)
    }
}
