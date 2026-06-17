use crate::error::CacheCatError;
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

// SENTINEL MASTERS
pub struct SentinelMastersCommand;

#[async_trait]
impl SubCommand for SentinelMastersCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        _items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let master_name = server.app.config.sentinel_master_name.to_string();
        let leader_endpoint = server.app.cluster.leader_addr().await;
        let mut flags = "master";
        let endpoint = match leader_endpoint {
            None => {
                flags = "master,o_down,disconnected";
                server.app.config.raft_advertise_endpoint.clone()
            }
            Some(endpoint) => endpoint,
        };
        let nodes_num = server.app.cluster.nodes().len();

        let mut result = Vec::new();

        let master_info = vec![
            Value::from_bluk_static_string("name"),
            Value::BulkString(Some(master_name.into())),
            Value::from_bluk_static_string("ip"),
            Value::BulkString(Some(Bytes::copy_from_slice(endpoint.addr().as_bytes()))),
            Value::from_bluk_static_string("port"),
            Value::BulkString(Some(endpoint.redis_port().to_string().into())),
            Value::from_bluk_static_string("flags"),
            Value::BulkString(Some(flags.to_string().into())),
            Value::from_bluk_static_string("num-other-sentinels"),
            Value::BulkString(Some(nodes_num.to_string().into())),
        ];

        result.push(Value::Array(Some(master_info)));

        Ok(Value::Array(Some(result)))
    }
}
