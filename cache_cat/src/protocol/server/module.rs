//! MODULE LIST reports the fixed set of built-in modules.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::protocol::{modules, option_eq};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::request::{Operation, RedisOperation};
use async_trait::async_trait;

pub struct ModuleCommand;

impl ModuleCommand {
    fn validate(items: &[Value]) -> Result<(), ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("module"));
        }
        let subcommand = match &items[1] {
            Value::BulkString(Some(bytes)) => bytes.as_ref(),
            Value::SimpleString(name) => name.as_bytes(),
            _ => return Err(ProtocolError::InvalidArgument("subcommand")),
        };
        if items.len() == 2 && option_eq(subcommand, b"LIST") {
            return Ok(());
        }

        // Redis 6.2 uses strcasecmp/%s for subcommands, including NUL termination.
        let name = subcommand
            .split(|&byte| byte == 0)
            .next()
            .unwrap_or_default();
        Err(ProtocolError::response(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'. Try MODULE HELP.",
            String::from_utf8_lossy(name),
        )))
    }
}

#[async_trait]
impl Command for ModuleCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        _server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Redis 6.2 validates MODULE's top-level arity before queueing, but
        // dispatches subcommands at execution time, including their errors.
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("module").into());
        }
        let response = match Self::validate(items) {
            Ok(()) => modules::list(),
            Err(error) => Value::from(error),
        };
        if let Some(queue) = client.transaction_queue.as_mut() {
            // The module set is fixed, so capturing this reply is deterministic.
            queue.push(Operation::Redis(RedisOperation::RedisReply(response)));
            return Ok(Value::queued());
        }
        Ok(response)
    }
}

impl RaftCommand for ModuleCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::response(
                "ERR Wrong number of args calling Redis command From Lua script",
            ));
        }
        // Redis rejects MODULE in Lua before dispatching its subcommand.
        Err(ProtocolError::response(
            "ERR This Redis command is not allowed from scripts",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(parts: &[&[u8]]) -> Vec<Value> {
        parts
            .iter()
            .map(|part| Value::BulkString(Some(Bytes::copy_from_slice(part))))
            .collect()
    }

    #[test]
    fn module_list_accepts_case_insensitive_c_string_subcommands() {
        for subcommand in [b"LIST".as_slice(), b"list", b"LiSt", b"LIST\0suffix"] {
            ModuleCommand::validate(&args(&[b"MODULE", subcommand])).unwrap();
        }
    }

    #[test]
    fn module_rejects_missing_unknown_and_extra_arguments() {
        for parts in [vec![], vec![b"MODULE".as_slice()]] {
            assert_eq!(
                Value::from(ModuleCommand::validate(&args(&parts)).unwrap_err()).encode(),
                b"-ERR wrong number of arguments for 'module' command\r\n",
            );
        }
        for (parts, name) in [
            (vec![b"MODULE".as_slice(), b"LIST", b"extra"], "LIST"),
            (vec![b"MODULE".as_slice(), b"LOAD", b"module.so"], "LOAD"),
            (vec![b"MODULE".as_slice(), b"UNLOAD", b"bf"], "UNLOAD"),
            (vec![b"MODULE".as_slice(), b"HELP"], "HELP"),
            (vec![b"MODULE".as_slice(), b"listx"], "listx"),
            (vec![b"MODULE".as_slice(), b"bad\0suffix"], "bad"),
        ] {
            assert_eq!(
                Value::from(ModuleCommand::validate(&args(&parts)).unwrap_err()).encode(),
                format!(
                    "-ERR Unknown subcommand or wrong number of arguments for '{name}'. Try MODULE HELP.\r\n"
                ).as_bytes(),
            );
        }
    }
}
