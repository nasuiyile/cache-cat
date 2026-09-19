use crate::error::ProtocolError;
use crate::raft::types::core::response_value::Value;
use bytes::Bytes;

pub mod xadd;
pub mod xread;

pub mod xreadgroup;

pub mod xgroup;

pub(super) fn arg(value: &Value) -> Result<Bytes, ProtocolError> {
    value
        .string_bytes_clone()
        .ok_or(ProtocolError::InvalidArgument("argument"))
}
