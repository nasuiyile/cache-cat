use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};

pub struct SentinelSlavesCommand;
#[async_trait]
impl SubCommand for SentinelSlavesCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL SLAVES").into());
        }

        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };

        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }

        let slaves = server.app.cluster.last_slave();

        let current_node_id: Bytes = server.app.cluster.node_id().to_string().into();
        let leader_info = server.app.cluster.leader_addr().await;
        let last_leader = server.app.cluster.last_leader();

        let last_addr: Bytes = last_leader.addr().to_string().into();
        let last_port: Bytes = last_leader.port().to_string().into();

        let result = stream::iter(slaves)
            .then(async |slave| {
                vec![
                    Bytes::from_static(b"name"),
                    slave.endpoint.redis_addr().into(),
                    Bytes::from_static(b"ip"),
                    slave.endpoint.addr().to_string().into(),
                    Bytes::from_static(b"port"),
                    slave.endpoint.redis_port().to_string().into(),
                    Bytes::from_static(b"runid"),
                    current_node_id.clone(),
                    Bytes::from_static(b"flags"),
                    if server.app.cluster.is_survive(slave.node_id).await {
                        Bytes::from_static(b"slave")
                    } else {
                        Bytes::from_static(b"slave,o_down,disconnected")
                    },
                    Bytes::from_static(b"master-link-status"),
                    if leader_info.is_some() {
                        Bytes::from_static(b"ok")
                    } else {
                        Bytes::from_static(b"err")
                    },
                    Bytes::from_static(b"master-host"),
                    last_addr.clone(),
                    Bytes::from_static(b"master-port"),
                    last_port.clone(),
                    Bytes::from_static(b"slave-repl-offset"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"slave-priority"),
                    Bytes::from_static(b"100"),
                ]
                .into_iter()
                .map(|v| Value::BulkString(Some(v)))
                .collect::<Vec<_>>()
            })
            .map(|slave_info| Value::Array(Some(slave_info)))
            .collect::<Vec<_>>()
            .await;

        Ok(Value::Array(Some(result)))
    }
}
