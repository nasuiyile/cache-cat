use crate::protocol::bf::error::{BloomOperation, from_engine};
use crate::raft::types::core::mocha::bloom_filter::{BloomError, BloomObject};
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

pub mod bf_add;
pub mod bf_exits;

pub mod bf_card;
pub mod bf_info;
pub mod bf_insert;
pub mod bf_loadchunk;
pub mod bf_madd;
pub mod bf_mexits;
pub mod bf_reserve;
pub mod bf_scandump;
mod error;

/// Shared per-item insertion behavior used by BF.MADD and BF.INSERT.
pub(super) fn add_items(bloom: &mut BloomObject, items: &[Bytes]) -> (Vec<Value>, bool) {
    let mut replies = Vec::with_capacity(items.len());
    let mut mutated = false;
    for item in items {
        match bloom.add(item) {
            Ok(true) => {
                mutated = true;
                replies.push(Value::Boolean(true));
            }
            Ok(false) => replies.push(Value::Boolean(false)),
            Err(error) => {
                let is_full = matches!(error, BloomError::Full);
                replies.push(from_engine(error, BloomOperation::Insert).into());
                if is_full {
                    break;
                }
            }
        }
    }
    (replies, mutated)
}
