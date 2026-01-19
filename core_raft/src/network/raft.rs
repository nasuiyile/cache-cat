use crate::network::model::{Request, Response};
use crate::network::network::NetworkFactory;
use openraft::Config;
use std::sync::Arc;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        Entry = openraft::Entry<TypeConfig>,
);
pub type LogStore = crate::store::log::LogStore<TypeConfig>;
pub type StateMachineStore = crate::store::state_machine::StateMachineStore<TypeConfig>;
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1;
    let config = Arc::new(Config::default().validate().unwrap());
    let log_store = LogStore::default();
    let state_machine_store = StateMachineStore::default();
    let network = NetworkFactory {};
    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await?;
    Ok(())
}
