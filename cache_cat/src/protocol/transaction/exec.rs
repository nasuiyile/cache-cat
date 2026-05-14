use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation, Request};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecParams {
    pub operations: Vec<Operation>,
}
impl Display for ExecParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        //如果 没有开启事务
        let params = match client.transaction_queue.take() {
            None => {
                return Err(ProtocolError::Custom(
                    "EXECABORT Transaction discarded because of previous errors.",
                )
                .into());
            }
            Some(queue) => RedisOperation::RedisExec(ExecParams { operations: queue }),
        };
        let value = server
            .app
            .write(Operation::Redis(params), client.db_number)
            .await?;
        Ok(value)
    }
}
