use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::SAdd;
use crate::raft::types::entry::bae_operation::SAddReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;

struct SAddArgs {
    key: Bytes,
    members: Vec<Bytes>,
}

pub struct SAddCommand;

impl SAddCommand {
    fn parse_args(items: &[Value]) -> Result<SAddArgs, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("sadd"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let members = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_unchecked)
            .cloned()
            .collect::<Vec<_>>();

        if members.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("member"));
        }

        Ok(SAddArgs { key, members })
    }
}

impl RaftCommand for SAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Base(SAdd(SAddReq {
            key: params.key,
            elements: params.members,
        })))
    }
}

#[async_trait]
impl Command for SAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::from_static_string("QUEUED"));
        }
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
