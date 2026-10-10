pub mod core;
mod info;

mod setinfo;
mod setname;

use crate::error::ProtocolError;
use crate::raft::types::core::response_value::Value;

pub(super) fn parse_client_name(value: &Value) -> Result<String, ProtocolError> {
    let name = value.as_str_lossy().ok_or(ProtocolError::SyntaxError)?;
    if !name.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(ProtocolError::response(
            "ERR Client names cannot contain spaces, newlines or special characters.",
        ));
    }
    // Redis accepts an empty name to clear a previously configured name.
    Ok(name.into_owned())
}
