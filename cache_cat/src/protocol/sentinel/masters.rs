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

        let master_info = vec![
            Bytes::from_static(b"name"),
            master_name.into(),
            Bytes::from_static(b"ip"),
            endpoint.addr().to_string().into(),
            Bytes::from_static(b"port"),
            endpoint.redis_port().to_string().into(),
            Bytes::from_static(b"flags"),
            flags.to_string().into(),
            Bytes::from_static(b"num-other-sentinels"),
            nodes_num.to_string().into(),
        ]
        .into_iter()
        .map(|v| Value::BulkString(Some(v)))
        .collect::<Vec<_>>();

        let result = vec![Value::Array(Some(master_info))];

        Ok(Value::Array(Some(result)))
    }
}
