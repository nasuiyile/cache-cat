use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;

use async_trait::async_trait;
use bytes::Bytes;

pub struct MemoryHelpCommand;

impl MemoryHelpCommand {
    fn parse(items: &[Value]) -> Result<(), ProtocolError> {
        if items.len() != 2 {
            return Err(ProtocolError::WrongArgCount("MEMORY HELP"));
        }

        let memory = items[0]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("command"))?;

        if !memory.as_ref().eq_ignore_ascii_case(b"MEMORY") {
            return Err(ProtocolError::InvalidArgument("command"));
        }

        let help = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("subcommand"))?;

        if !help.as_ref().eq_ignore_ascii_case(b"HELP") {
            return Err(ProtocolError::InvalidArgument("subcommand"));
        }

        Ok(())
    }

    fn help_line(line: &'static str) -> Value {
        Value::BulkString(Some(Bytes::from_static(line.as_bytes())))
    }

    fn help() -> Value {
        Value::Array(Some(vec![
            Self::help_line(
                "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            ),
            Self::help_line("DOCTOR"),
            Self::help_line("    Return memory problems reports."),
            Self::help_line("MALLOC-STATS"),
            Self::help_line(
                "    Return internal statistics report from the memory allocator.",
            ),
            Self::help_line("PURGE"),
            Self::help_line(
                "    Attempt to purge dirty pages for reclamation by the allocator.",
            ),
            Self::help_line("STATS"),
            Self::help_line(
                "    Return information about the memory usage of the server.",
            ),
            Self::help_line("USAGE <key> [SAMPLES <count>]"),
            Self::help_line(
                "    Return memory in bytes used by <key> and its value. Nested values are",
            ),
            Self::help_line(
                "    sampled up to <count> times (default: 5, 0 means sample all).",
            ),
            Self::help_line("HELP"),
            Self::help_line("    Prints this help."),
        ]))
    }
}

#[async_trait]
impl SubCommand for MemoryHelpCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Self::parse(items)?;

        Ok(Self::help())
    }
}