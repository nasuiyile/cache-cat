use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::Operation;
use crate::raft::types::entry::request::RedisOperation::RedisSet;
use async_trait::async_trait;

use super::set::{Expiration, SetParams};

pub struct SetExCommand;

impl SetExCommand {
    fn parse(items: &[Value]) -> Result<SetParams, ProtocolError> {
        // SETEX key seconds value
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("setex"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let seconds = items[2].try_parse_u64()?;

        let value = items[3]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("value"))?
            .clone();

        Ok(SetParams {
            key,
            value,
            mode: None,
            get: false,
            // 秒转毫秒，继续复用 Px
            expiration: Some(Expiration::Px(seconds * 1000)),
        })
    }
}

impl RaftCommand for SetExCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse(items)?;
        Ok(Operation::Redis(RedisSet(params)))
    }
}

#[async_trait]
impl Command for SetExCommand {
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

        let params = Self::parse(items)?;
        server
            .app
            .write(Operation::Redis(RedisSet(params)), client.db_number)
            .await?;

        Ok(Value::ok())
    }
}
