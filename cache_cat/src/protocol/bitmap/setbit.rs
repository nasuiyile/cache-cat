use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::BaseOperation::SetBit;
use crate::raft::types::entry::bae_operation::SetBitReq;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetBitParams {
    pub key: Bytes,
    pub offset: u64,
    pub value: u8, // 0 或 1
}

impl Display for SetBitParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SetBitParams {{ key: {:?}, offset: {}, value: {} }}",
            self.key, self.offset, self.value
        )
    }
}

pub struct SetBitCommand;

impl SetBitCommand {
    fn parse_args(items: &[Value]) -> Result<SetBitParams, ProtocolError> {
        if items.len() != 4 {
            return Err(ProtocolError::WrongArgCount("setbit"));
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("setbit")),
        };

        let offset = match &items[2] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                match s.parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(ProtocolError::Custom(
                            "ERR bit offset is not an integer or out of range",
                        ));
                    }
                }
            }
            Value::SimpleString(s) => match s.parse::<u64>() {
                Ok(v) => v,
                Err(_) => {
                    return Err(ProtocolError::Custom(
                        "ERR bit offset is not an integer or out of range",
                    ));
                }
            },
            Value::Integer(i) => {
                if *i < 0 {
                    return Err(ProtocolError::Custom(
                        "ERR bit offset is not an integer or out of range",
                    ));
                }
                *i as u64
            }
            _ => {
                return Err(ProtocolError::Custom(
                    "ERR bit offset is not an integer or out of range",
                ));
            }
        };

        let value = match &items[3] {
            Value::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                match s.parse::<u8>() {
                    Ok(v) if v <= 1 => v,
                    _ => {
                        return Err(ProtocolError::Custom(
                            "ERR bit is not an integer or out of range",
                        ));
                    }
                }
            }
            Value::SimpleString(s) => match s.parse::<u8>() {
                Ok(v) if v <= 1 => v,
                _ => {
                    return Err(ProtocolError::Custom(
                        "ERR bit is not an integer or out of range",
                    ));
                }
            },
            Value::Integer(i) => {
                if *i < 0 || *i > 1 {
                    return Err(ProtocolError::Custom(
                        "ERR bit is not an integer or out of range",
                    ));
                }
                *i as u8
            }
            _ => {
                return Err(ProtocolError::Custom(
                    "ERR bit is not an integer or out of range",
                ));
            }
        };

        Ok(SetBitParams {
            key: key.into(),
            offset,
            value,
        })
    }
}

impl RaftCommand for SetBitCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = SetBitCommand::parse_args(items)?;
        Ok(Operation::Base(SetBit(SetBitReq {
            key: params.key,
            offset: params.offset,
            value: params.value,
        })))
    }
}

#[async_trait]
impl Command for SetBitCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("SETBIT")));
        }
        // Build raft operation
        let operation = self.raft_request(items)?; // Execute write
        let value = server.app.write(operation, client.db_number).await?;
        Ok(value)
    }
}
