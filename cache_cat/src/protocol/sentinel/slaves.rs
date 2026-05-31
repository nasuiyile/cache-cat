use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

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
        let current_node_id = server.app.cluster.node_id();
        let mut result = Vec::new();
        let leader_info = server.app.cluster.leader_addr().await;
        let last_leader = server.app.cluster.last_leader();

        for slave in slaves {
            let mut slave_info = Vec::new();
            slave_info.push(Value::BulkString(Some(b"name".to_vec())));
            slave_info.push(Value::BulkString(Some(slave.endpoint.redis_addr().into_bytes())));
            slave_info.push(Value::BulkString(Some(b"ip".to_vec())));
            slave_info.push(Value::BulkString(Some(
                slave.endpoint.addr().to_string().into_bytes(),
            )));
            slave_info.push(Value::BulkString(Some(b"port".to_vec())));
            slave_info.push(Value::BulkString(Some(
                slave.endpoint.redis_port().to_string().into_bytes(),
            )));
            slave_info.push(Value::BulkString(Some(b"runid".to_vec())));
            slave_info.push(Value::BulkString(Some(
                current_node_id.to_string().into_bytes(),
            )));
            slave_info.push(Value::BulkString(Some(b"flags".to_vec())));
            if server.app.cluster.is_survive(slave.node_id).await{
                slave_info.push(Value::BulkString(Some(b"slave".to_vec())));
            }else{
                slave_info.push(Value::BulkString(Some(b"slave,o_down,disconnected".to_vec())));
            }

            slave_info.push(Value::BulkString(Some(b"master-link-status".to_vec())));
            if leader_info.is_some() {
                slave_info.push(Value::BulkString(Some(b"ok".to_vec())));
            } else {
                slave_info.push(Value::BulkString(Some(b"err".to_vec())));
            }

            slave_info.push(Value::BulkString(Some(b"master-host".to_vec())));
            slave_info.push(Value::BulkString(Some(
                last_leader.addr().to_string().into_bytes(),
            )));
            slave_info.push(Value::BulkString(Some(b"master-port".to_vec())));
            slave_info.push(Value::BulkString(Some(
                last_leader.port().to_string().into_bytes(),
            )));

            slave_info.push(Value::BulkString(Some(b"slave-repl-offset".to_vec())));
            slave_info.push(Value::BulkString(Some(b"0".to_vec())));

            slave_info.push(Value::BulkString(Some(b"slave-priority".to_vec())));
            slave_info.push(Value::BulkString(Some(b"100".to_vec())));

            result.push(Value::Array(Some(slave_info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
