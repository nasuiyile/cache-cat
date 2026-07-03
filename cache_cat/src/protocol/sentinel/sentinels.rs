use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};

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

        // If the master name does not match, return an empty array directly
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }

        let result = stream::iter(server.app.cluster.nodes())
            .then(async move |(node_id, node)| {
                let shared_node_id: Bytes = node_id.to_string().into();

                vec![
                    Bytes::from_static(b"name"),
                    // Here returns node_id as the name of the sentinel
                    shared_node_id.clone(),
                    Bytes::from_static(b"ip"),
                    node.endpoint.addr().to_string().into(),
                    Bytes::from_static(b"port"),
                    node.endpoint.redis_port().to_string().into(),
                    Bytes::from_static(b"runid"),
                    shared_node_id,
                    Bytes::from_static(b"flags"),
                    if server.app.cluster.is_survive(node_id).await {
                        Bytes::from_static(b"sentinel")
                    } else {
                        Bytes::from_static(b"sentinel,o_down,disconnected")
                    },
                ]
                .into_iter()
                .map(|v| Value::BulkString(Some(v)))
                .collect::<Vec<_>>()
            })
            .map(|info| Value::Array(Some(info)))
            .collect::<Vec<_>>()
            .await;

        Ok(Value::Array(Some(result)))
    }
}
