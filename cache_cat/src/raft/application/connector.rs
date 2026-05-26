use crate::error::{CacheCatError, RpcError};
use crate::raft::network::client::RpcMultiClient;
use crate::raft::types::raft_types::TypeConfig;
use openraft::error::Timeout;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct Connector {
    connection: RwLock<HashMap<String, RpcMultiClient>>,
}

impl Connector {
    pub fn new() -> Self {
        Connector {
            connection: RwLock::new(HashMap::new()),
        }
    }

    pub async fn send_msg<Req, Res>(
        &self,
        addr: &str,
        func_id: u32,
        req: Req,
        duration: Duration,
        err: Timeout<TypeConfig>,
    ) -> Result<Res, CacheCatError>
    where
        Req: Serialize + Send,
        Res: DeserializeOwned + Send,
    {
        let client = {
            let guard = self.connection.read().await;
            guard.get(addr).cloned()
        };
        let client = match client {
            Some(c) => c,
            None => {
                let new_client = RpcMultiClient::connect_with_num(addr, 1)
                    .await
                    .map_err(|e| RpcError::Network(e.to_string()))?;
                self.connection
                    .write()
                    .await
                    .insert(addr.to_string(), new_client.clone());
                new_client
            }
        };
        let result = client
            .call_with_timeout::<Req, Res>(func_id, req, duration, err)
            .await?;
        Ok(result)
    }
}
