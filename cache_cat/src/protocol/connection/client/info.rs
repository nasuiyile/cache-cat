use crate::error::CacheCatError;
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::connection::Connection;
use crate::raft::network::redis_server::{RedisServer, RespCodec};
use crate::raft::types::core::response_value::Value;
use crate::utils::now_ms;
use async_trait::async_trait;
use std::collections::HashMap;
#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(windows)]
use std::os::windows::io::AsRawSocket;
use tokio_util::codec::Framed;

#[allow(clippy::unnecessary_cast)]
fn connection_id(framed: &Framed<Connection, RespCodec>) -> u64 {
    let conn = framed.get_ref();
    #[cfg(unix)]
    {
        conn.as_raw_fd() as u64
    }

    #[cfg(windows)]
    {
        conn.as_raw_socket() as u64
    }
}

fn map_to_string(map: &HashMap<String, String>) -> String {
    map.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(" ")
}

pub struct ClientInfoCommand;

#[async_trait]
impl SubCommand for ClientInfoCommand {
    async fn execute(
        &self,
        client: &mut Client,
        _items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("id".to_string(), client.id.to_string());
        let client_addr = client.framed.get_ref().as_stream().peer_addr()?.to_string();
        // client.framed.get_ref().peer_addr()?.port().to_string();
        map.insert("addr".to_string(), client_addr);

        let local_addr = client
            .framed
            .get_ref()
            .as_stream()
            .local_addr()?
            .to_string();
        map.insert("laddr".to_string(), local_addr);

        let fd = connection_id(&client.framed);
        map.insert("fd".to_string(), fd.to_string());

        map.insert("name".to_string(), client.name.to_string());

        let age = (now_ms() - client.connection_time) / 1000;
        map.insert("age".to_string(), age.to_string());

        let idle = (now_ms() - client.last_interaction) / 1000;
        map.insert("idle".to_string(), idle.to_string());

        map.insert("flags".to_string(), client.flag.to_string());

        map.insert("db".to_string(), client.db_number.to_string());

        map.insert("cmd".to_string(), client.last_cmd.to_string());

        map.insert("lib_name".to_string(), client.lib_name.to_string());
        map.insert("lib_ver".to_string(), client.lib_ver.to_string());

        let sub = server.broadcast.client_subscription_count(client.id).await;
        map.insert("sub".to_string(), sub.to_string());

        let psub = server.broadcast.client_pattern_count(client.id).await;
        map.insert("psub".to_string(), psub.to_string());

        let res = map_to_string(&map);

        Ok(Value::BulkString(Some(res.into())))
    }
}
