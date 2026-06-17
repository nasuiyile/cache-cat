use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

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

        let name = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("master name"))?;
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }
        let slaves = server.app.cluster.last_slave();
        let current_node_id = server.app.cluster.node_id();
        let mut result = Vec::new();
        let leader_info = server.app.cluster.leader_addr().await;
        let last_leader = server.app.cluster.last_leader();

        for slave in slaves {
            let mut slave_info = vec![
                Value::from_bluk_static_string("name"),
                Value::BulkString(Some(slave.endpoint.redis_addr().into())),
                Value::from_bluk_static_string("ip"),
                Value::BulkString(Some(Bytes::copy_from_slice(
                    slave.endpoint.addr().as_bytes(),
                ))),
                Value::from_bluk_static_string("port"),
                Value::BulkString(Some(slave.endpoint.redis_port().to_string().into())),
                Value::from_bluk_static_string("runid"),
                Value::BulkString(Some(current_node_id.to_string().into())),
                Value::from_bluk_static_string("flags"),
            ];

            if server.app.cluster.is_survive(slave.node_id).await {
                slave_info.push(Value::from_bluk_static_string("slave"));
            } else {
                slave_info.push(Value::from_bluk_static_string("slave,o_down,disconnected"));
            }

            slave_info.push(Value::from_bluk_static_string("master-link-status"));
            if leader_info.is_some() {
                slave_info.push(Value::from_bluk_static_string("ok"));
            } else {
                slave_info.push(Value::from_bluk_static_string("err"));
            }

            slave_info.extend([
                Value::from_bluk_static_string("master-host"),
                Value::BulkString(Some(Bytes::copy_from_slice(last_leader.addr().as_bytes()))),
                Value::from_bluk_static_string("master-port"),
                Value::BulkString(Some(last_leader.port().to_string().into())),
                Value::from_bluk_static_string("slave-repl-offset"),
                Value::from_bluk_static_string("0"),
                Value::from_bluk_static_string("slave-priority"),
                Value::from_bluk_static_string("100"),
            ]);

            result.push(Value::Array(Some(slave_info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
