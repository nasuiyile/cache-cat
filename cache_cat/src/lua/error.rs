use mlua::prelude::LuaError;

use crate::error::{Error, ProtocolError};

impl From<LuaError> for ProtocolError {
    fn from(err: LuaError) -> Self {
        match err {
            LuaError::SyntaxError { message, .. } => ProtocolError::ScriptCompileError(message),
            LuaError::RuntimeError(message) => ProtocolError::ScriptError("f_script", message),
            LuaError::MemoryError(message) => ProtocolError::ScriptError("unknown", message),
            // ... 其他 Lua 错误类型
            _ => ProtocolError::ScriptError("unknown", err.to_string().replace('\n', " ")),
        }
    }
}

// 这样 Error 的转换链就自动工作了
impl From<LuaError> for Error {
    fn from(err: LuaError) -> Self {
        // LuaError -> ProtocolError -> ErrorKind -> Error
        ProtocolError::from(err).into()
    }
}
