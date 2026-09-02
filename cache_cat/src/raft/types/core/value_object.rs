use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::raft::types::core::mocha::bloom_filter::BloomObject;
use crate::raft::types::core::size_estimate::{
    estimate_bloom_usage, estimate_hash_usage, estimate_list_usage, estimate_set_usage,
    estimate_zset_usage, estimated_bytes_heap_usage,
};
use crate::raft::types::core::sorted_set::SortedSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HashValue {
    Str(Bytes),
    Int(i64),
}

impl HashValue {
    pub(crate) fn to_bytes(&self) -> Bytes {
        match self {
            HashValue::Str(value) => value.clone(),
            HashValue::Int(value) => value.to_string().into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),

    String(Bytes),

    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Bytes>>>),

    #[serde(with = "mutex_hashmap_serde")]
    Hash(Arc<Mutex<HashMap<Bytes, HashValue>>>),

    #[serde(with = "mutex_zset_serde")]
    ZSet(Arc<Mutex<SortedSet>>),

    #[serde(with = "mutex_hashset_serde")]
    Set(Arc<Mutex<HashSet<Bytes>>>),

    #[serde(with = "mutex_bloom_serde")]
    Bloom(Arc<Mutex<BloomObject>>),
}

impl ValueObject {
    pub fn estimated_memory_usage(&self, samples: usize) -> usize {
        size_of::<Self>().saturating_add(self.estimated_heap_usage(samples))
    }

    pub fn estimated_heap_usage(&self, samples: usize) -> usize {
        match self {
            ValueObject::Int(_) => 0,

            ValueObject::String(value) => estimated_bytes_heap_usage(value),

            ValueObject::List(value) => estimate_list_usage(value, samples),

            ValueObject::Hash(value) => estimate_hash_usage(value, samples),

            ValueObject::ZSet(value) => estimate_zset_usage(value, samples),

            ValueObject::Set(value) => estimate_set_usage(value, samples),

            ValueObject::Bloom(value) => estimate_bloom_usage(value),
        }
    }
}

// 通用 Arc<Mutex<T>> serde 实现宏
macro_rules! impl_mutex_serde {
    ($mod_name:ident, $inner_type:ty) => {
        mod $mod_name {
            use super::*;
            use serde::de::Deserializer;
            use serde::{Deserialize, Serialize};

            pub fn serialize<S>(
                data: &Arc<Mutex<$inner_type>>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let guard = data.lock();

                guard.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Mutex<$inner_type>>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$inner_type>::deserialize(deserializer)?;

                Ok(Arc::new(Mutex::new(value)))
            }
        }
    };
}

impl_mutex_serde!(mutex_vecdeque_serde, VecDeque<Bytes>);

impl_mutex_serde!(
    mutex_hashmap_serde,
    HashMap<Bytes, HashValue>
);

impl_mutex_serde!(mutex_zset_serde, SortedSet);

impl_mutex_serde!(mutex_hashset_serde, HashSet<Bytes>);

impl_mutex_serde!(mutex_bloom_serde, BloomObject);
