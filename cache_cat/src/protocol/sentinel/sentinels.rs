use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Client;
use crate::protocol::sentinel::sentinel::SubCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

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

        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };

        // master name 不匹配直接返回空数组
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }
        let nodes = server.app.cluster.nodes();
        let mut result = Vec::new();
        for (node_id, node) in nodes {
            let mut info = Vec::new();
            info.push(Value::BulkString(Some(b"name".to_vec())));
            //这里返回node_id作为哨兵的名字
            info.push(Value::BulkString(Some(
                node_id.to_string().into_bytes(),
            )));

            info.push(Value::BulkString(Some(b"ip".to_vec())));
            info.push(Value::BulkString(Some(
                node.endpoint.addr().to_string().into_bytes(),
            )));

            info.push(Value::BulkString(Some(b"port".to_vec())));
            info.push(Value::BulkString(Some(
                node.endpoint.redis_port().to_string().into_bytes(),
            )));

            info.push(Value::BulkString(Some(b"runid".to_vec())));
            info.push(Value::BulkString(Some(
                node_id.to_string().into_bytes(),
            )));

            info.push(Value::BulkString(Some(b"flags".to_vec())));
            if server.app.cluster.is_survive(node_id).await{
                info.push(Value::BulkString(Some(b"sentinel".to_vec())));
            }else{
                info.push(Value::BulkString(Some(b"sentinel,o_down,disconnected".to_vec())));
            }

            result.push(Value::Array(Some(info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
