//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::network::model::Value;
use crate::protocol::command::Command;
use crate::server::handler::rpc::Server;
use crate::util::now_ms;
use async_trait::async_trait;

/// DEL command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct DelParams {
    pub keys: Vec<Vec<u8>>,
}

impl DelParams {
    /// Parse DEL command parameters from RESP array items
    /// Format: DEL key [key ...]
    fn parse(items: &[Value]) -> Option<Self> {
        // Need at least: DEL key (2 items)
        if items.len() < 2 {
            return None;
        }

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(items.len() - 1);
        for item in items.iter().skip(1) {
            let key = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return None,
            };
            keys.push(key);
        }

        Some(DelParams { keys })
    }
}

/// DEL command executor
pub struct DelCommand;

#[async_trait]
impl Command for DelCommand {
    async fn execute(&self, items: &[Value], server: &Server) -> Value {
        let params = match DelParams::parse(items) {
            Some(params) => params,
            None => return Value::error("ERR wrong number of arguments for 'del' command"),
        };

        let mut deleted_count = 0i64;

        // 计算每一个key的哈希分组，然后将对应的删除命令提交到对应的group



        Value::Integer(deleted_count)
    }
}
