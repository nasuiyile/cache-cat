use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::protocol::string::set::{SetMode, SetParams};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisSet;
use async_trait::async_trait;

/// SETNX command executor
pub struct SetNxCommand;

impl SetNxCommand {
    /// Parse SETNX command parameters
    ///
    /// Format:
    /// SETNX key value
    fn parse(items: &[Value]) -> Result<SetParams, ProtocolError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("setnx"));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        let value = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("value")),
        };

        Ok(SetParams {
            key,
            value,
            mode: Some(SetMode::Nx),
            get: false,
            expiration: None,
        })
    }
}

impl RaftCommand for SetNxCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Redis(RedisSet(params)))
    }
}

#[async_trait]
impl Command for SetNxCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }
        let params = Self::parse(items)?;
        server
            .app
            .write(Operation::Redis(RedisSet(params)), client.db_number)
            .await
    }
}
