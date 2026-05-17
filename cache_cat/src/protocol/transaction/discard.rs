use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct DiscardCommand;

#[async_trait]
impl Command for DiscardCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // DISCARD 不接受额外参数
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("DISCARD").into());
        }

        // 必须先开启 MULTI
        if client.transaction_queue.is_none() {
            return Err(
                ProtocolError::Custom("DISCARD without MULTI").into()
            );
        }

        // 清空事务队列
        client.transaction_queue = None;

        Ok(Value::ok())
    }
}