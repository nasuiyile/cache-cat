use crate::error::{CacheCatError, RpcError};
use crate::raft::network::client::RpcMultiClient;
use crate::raft::types::raft_types::TypeConfig;
use openraft::error::{RPCError, Timeout};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

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
        addr: String,
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
            guard.get(&addr).cloned()
        };

        let client = match client {
            Some(c) => c,
            None => {
                let connect_future = RpcMultiClient::connect_with_num(&addr, 1);

                let new_client = match timeout(duration, connect_future).await {
                    Ok(result) => {
                        result.map_err(|e| RpcError::Network(e.to_string()))?
                    }
                    Err(_) => {
                        return Err(RPCError::Timeout(err).into());
                    }
                };
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
