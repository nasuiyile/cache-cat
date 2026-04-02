//! DEL command implementation
//!
//! DEL key [key ...]
//! Removes the specified keys. A key is ignored if it does not exist.
//!
//! Returns:
//! - The number of keys that were removed
//! - 0 if none of the specified keys existed

use crate::network::model::BaseOperation::Del;
use crate::network::model::{Request, Value};
use crate::network::node::{get_app, get_group_id_by_key};
use crate::protocol::command::Command;
use crate::server::handler::model::DelReq;
use crate::server::handler::rpc::Server;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// DEL command parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
impl Display for DelParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelReq {{ keys: {:?} }}", self.keys)
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

        let mut map = HashMap::new();
        // 计算每一个key的哈希分组，然后将对应的删除命令提交到对应的group 这是为了让每一个group中的提交都是原子的
        for key in params.keys {
            let group_id = get_group_id_by_key(&key);
            map.entry(group_id).or_insert_with(Vec::new).push(key);
        }
        for (group_id, keys) in map {
            let app = get_app(&server.app, group_id);
            let request = Request::Base(Del(DelReq {
                keys: Arc::from(keys),
            }));
            match app.raft.client_write(request).await {
                Ok(res) => match res.data {
                    Value::Integer(i) => {
                        deleted_count += i;
                    }
                    _ => return Value::error("ERR unexpected response"),
                },
                Err(e) => return Value::error(format!("ERR {}", e)),
            }
        }

        Value::Integer(deleted_count)
    }
}
