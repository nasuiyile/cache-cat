use openraft::Entry;
use raft_engine::{Config, Engine, MessageExt, ReadableSize};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use crate::raft::types::raft_types::TypeConfig;

pub fn create_raft_engine<P: AsRef<Path>>(path: P) -> Arc<Engine> {
    let path = path.as_ref().to_string_lossy().into_owned();
    let config = Config {
        dir: path,
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    Arc::new(Engine::open(config).expect("Open raft engine"))
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry<TypeConfig>;

    fn index(e: &Self::Entry) -> u64 {
        e.log_id.index
    }
}
