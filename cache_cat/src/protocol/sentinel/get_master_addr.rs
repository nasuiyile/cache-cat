use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
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

        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::BulkString(None));
        }
        let last_leader = server.app.cluster.last_leader();
        info!("get master addr by name: {}", last_leader);
        Ok(Value::Array(Some(vec![
            Value::BulkString(Some(last_leader.addr().to_string().into_bytes())),
            Value::BulkString(Some(last_leader.redis_port().to_string().into_bytes())),
        ])))
    }
}
