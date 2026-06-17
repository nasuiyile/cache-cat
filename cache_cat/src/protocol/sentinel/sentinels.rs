use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

pub struct SentinelSentinelsCommand;

#[async_trait]
impl SubCommand for SentinelSentinelsCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // SENTINEL SENTINELS <master-name>
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL SENTINELS").into());
        }

        let name = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("master name"))?;

        // master name 不匹配直接返回空数组
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }
        let nodes = server.app.cluster.nodes();
        let mut result = Vec::new();
        for (node_id, node) in nodes {
            let mut info = vec![
                Value::from_bluk_static_string("name"),
                //这里返回node_id作为哨兵的名字
                Value::BulkString(Some(node_id.to_string().into())),
                Value::from_bluk_static_string("ip"),
                Value::BulkString(Some(Bytes::copy_from_slice(
                    node.endpoint.addr().as_bytes(),
                ))),
                Value::from_bluk_static_string("port"),
                Value::BulkString(Some(node.endpoint.redis_port().to_string().into())),
                Value::from_bluk_static_string("runid"),
                Value::BulkString(Some(node_id.to_string().into())),
                Value::from_bluk_static_string("flags"),
            ];

            if server.app.cluster.is_survive(node_id).await {
                info.push(Value::from_bluk_static_string("sentinel"));
            } else {
                info.push(Value::from_bluk_static_string(
                    "sentinel,o_down,disconnected",
                ));
            }

            result.push(Value::Array(Some(info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
