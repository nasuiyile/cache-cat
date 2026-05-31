use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct MultiCommand;

#[async_trait]
impl Command for MultiCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() >= 2 {
            return Err(ProtocolError::WrongArgCount("MULTI").into());
        }
        //如果已经被打开过了
        if client.transaction_queue.is_some() {
            return Err(ProtocolError::Custom("MULTI calls can not be nested").into());
        }
        client.transaction_queue = Some(vec![]);
        client.flag.multi = true;
        Ok(Value::ok())
    }
}
