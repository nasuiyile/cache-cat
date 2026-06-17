use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;
use tracing::info;

// SENTINEL GET-MASTER-ADDR-BY-NAME <name>
pub struct SentinelGetMasterAddrByNameCommand;

#[async_trait]
impl SubCommand for SentinelGetMasterAddrByNameCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL GET-MASTER-ADDR-BY-NAME").into());
        }

        let name = items[2]
            .as_str_lossy()
            .ok_or(ProtocolError::InvalidArgument("master name"))?;

        if server.app.config.sentinel_master_name != name {
            return Ok(Value::BulkString(None));
        }
        let last_leader = server.app.cluster.last_leader();
        info!("get master addr by name: {}", last_leader);
        Ok(Value::Array(Some(vec![
            Value::BulkString(Some(Bytes::copy_from_slice(last_leader.addr().as_bytes()))),
            Value::BulkString(Some(last_leader.redis_port().to_string().into())),
        ])))
    }
}
