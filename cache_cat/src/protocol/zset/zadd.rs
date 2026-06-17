use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::ZAdd;
use crate::raft::types::entry::bae_operation::ZAddReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZAddParam {
    pub key: Bytes,
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
    pub ch: bool,
    pub members: Vec<(Bytes, f64)>,
}

impl Display for ZAddParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZAddParam {{ key: {}, nx: {}, xx: {}, gt: {}, lt: {}, ch: {}, members: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.nx,
            self.xx,
            self.gt,
            self.lt,
            self.ch,
            self.members
        )
    }
}

pub struct ZAddCommand;

impl ZAddCommand {
    fn parse_params(items: &[Value]) -> Result<ZAddParam, ProtocolError> {
        // Minimum: ZADD key score member (4 items)
        if items.len() < 4 {
            return Err(ProtocolError::WrongArgCount("zadd"));
        }

        let key = items[1]
            .string_bytes_unchecked()
            .ok_or(ProtocolError::InvalidArgument("key"))?
            .clone();

        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;

        // Parse flags from items[2..] until we hit a score (number)
        let mut i = 2;
        while i < items.len() {
            let Some(flag) = items[i].as_str_lossy() else {
                break;
            };

            match flag.to_uppercase().as_str() {
                "NX" => {
                    nx = true;
                    i += 1;
                }
                "XX" => {
                    xx = true;
                    i += 1;
                }
                "GT" => {
                    gt = true;
                    i += 1;
                }
                "LT" => {
                    lt = true;
                    i += 1;
                }
                "CH" => {
                    ch = true;
                    i += 1;
                }
                _ => break,
            }
        }

        if nx && xx {
            return Err(ProtocolError::Custom(
                "ERR XX and NX options at the same time are not compatible",
            ));
        }

        if gt && lt {
            return Err(ProtocolError::Custom(
                "ERR GT and LT options at the same time are not compatible",
            ));
        }

        // Remaining items must be score-member pairs
        let remaining = &items[i..];
        if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
            return Err(ProtocolError::WrongArgCount("zadd"));
        }

        let mut members = Vec::with_capacity(remaining.len() / 2);
        let mut j = 0;
        while j < remaining.len() {
            let score = remaining[j]
                .as_str_lossy()
                .and_then(|str| str.parse::<f64>().ok())
                .ok_or(ProtocolError::Custom("ERR value is not a valid float"))?;

            let member = items[j + 1]
                .string_bytes_unchecked()
                .ok_or(ProtocolError::InvalidArgument("member"))?
                .clone();

            members.push((member, score));
            j += 2;
        }

        Ok(ZAddParam {
            key,
            nx,
            xx,
            gt,
            lt,
            ch,
            members,
        })
    }

    #[allow(dead_code)]
    fn parse_score(data: &[u8]) -> Option<f64> {
        let s = String::from_utf8_lossy(data);
        s.parse::<f64>().ok()
    }
}
impl RaftCommand for ZAddCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_params(items)?;
        Ok(Operation::Base(ZAdd(ZAddReq {
            key: params.key,
            nx: params.nx,
            xx: params.xx,
            gt: params.gt,
            lt: params.lt,
            ch: params.ch,
            members: params.members,
        })))
    }
}

#[async_trait]
impl Command for ZAddCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Parse arguments
        let operation = self.raft_request(items)?;
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
