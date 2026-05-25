//! PUBLISH command implementation
//!
//! PUBLISH channel message
//! Posts a message to the given channel.
//!
//! Returns:
//! - The number of clients that received the message
//! - 0 if no clients are subscribed to the channel

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::raft::network::model::PublishReq;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// PUBLISH command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishParams {
    pub channel: Vec<u8>,
    pub message: Vec<u8>,
}

impl PublishParams {
    /// Parse PUBLISH command parameters from RESP array items
    /// Format: PUBLISH channel message
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need exactly: PUBLISH channel message (3 items)
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("publish"));
        }

        let channel = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::WrongArgCount("publish")),
        };

        let message = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::WrongArgCount("publish")),
        };

        Ok(PublishParams { channel, message })
    }
}

impl Display for PublishParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PublishReq {{ channel: {:?}, message: {:?} }}",
            self.channel, self.message
        )
    }
}

/// PUBLISH command executor
pub struct PublishCommand;

#[async_trait]
impl Command for PublishCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let params = PublishParams::parse(items)?;
        // 直接传入原始消息，让 publish_message 封装
        server
            .broadcast
            .publish_message(&params.channel, params.message.clone())
            .await;
        let req = PublishReq {
            channel: params.channel,
            message: params.message,
        };
        let s = server.clone();
        tokio::spawn(async move {
            _ = s.app.leader_rpc_call::<PublishReq, ()>(11, req).await;
        });

        Ok(Value::SimpleString(String::from("QUEUED")))
    }
}
