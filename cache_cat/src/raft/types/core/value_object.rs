use std::collections::{BTreeMap, HashMap, HashSet, LinkedList};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),

    String(Arc<Vec<u8>>),
    List(LinkedList<Arc<Vec<u8>>>),

    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
}
